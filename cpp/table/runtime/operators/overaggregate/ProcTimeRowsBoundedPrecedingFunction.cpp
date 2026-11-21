/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You can obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "ProcTimeRowsBoundedPrecedingFunction.h"

#include <chrono>
#include <climits>
#include <regex>
#include <utility>

#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "table/runtime/generated/GeneratedAggsHandleFunctionMinMax.h"
#include "core/typeutils/LongSerializer.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

namespace {

const char* const PROCESS_FUNCTION_NAME = "ProcTimeRowsBoundedPrecedingFunction";

// Same shape as ProcTimeUnboundedPrecedingFunction: restricted to the v1
// over-aggregate whitelist SUM / SUM0 / COUNT / MAX / MIN.
std::string extractAggName(const std::string& input)
{
    std::regex aggRegex(R"((MAX|COUNT|SUM|MIN))", std::regex_constants::icase);
    std::smatch match;
    if (std::regex_search(input, match, aggRegex)) {
        return match.str();
    }
    return "NONE";
}

struct OverAggCallMeta {
    int aggFuncIndex = -1;
    std::string aggTypeStr;
    std::string aggType;
    int filterIndex = -1;
    int aggIndex = -1; // -1: COUNT(*)
    std::vector<int> argIndexes;
    std::string aggDataType;
};

} // namespace

ProcTimeRowsBoundedPrecedingFunction::ProcTimeRowsBoundedPrecedingFunction(
    long stateRetentionTime, const nlohmann::json& config)
    : stateRetentionTime(stateRetentionTime)
{
    parseDescription(config);

    accumulatorArity = static_cast<int>(accTypes.size());
    aggValueArity = static_cast<int>(aggValueTypes.size());

    // Partition key selector over the grouping columns (empty grouping = one
    // global partition; the selector then produces an empty key row).
    std::vector<int32_t> keyedTypes;
    for (int index : groupingIndices) {
        if (index < 0 || index >= static_cast<int>(inputTypes.size())) {
            throw std::runtime_error(
                "ProcTimeRowsBoundedPrecedingFunction: grouping index out of range: " + std::to_string(index));
        }
        keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
    }
    groupByKeySelector = new KeySelector<RowData*>(keyedTypes, groupingIndices);
}

ProcTimeRowsBoundedPrecedingFunction::~ProcTimeRowsBoundedPrecedingFunction()
{
    for (auto* func : functions) {
        delete func;
    }
    for (auto* row : collectedRows) {
        delete row;
    }
    for (auto* value : collectedAggValues) {
        delete value;
    }
    delete groupByKeySelector;
    delete resultRow;
    delete reuseAggValue;
}

void ProcTimeRowsBoundedPrecedingFunction::parseDescription(const nlohmann::json& config)
{
    // Defense in depth: the factory only routes here for this very name; a
    // mismatching description must fail fast instead of mis-executing.
    if (!config.contains("processFunction") ||
        config["processFunction"].get<std::string>() != PROCESS_FUNCTION_NAME) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction: description processFunction mismatch: " + config.dump());
    }

    // Java: Preconditions.checkArgument(precedingOffset > 0).
    if (!config.contains("precedingOffset") || !config["precedingOffset"].is_number_integer()) {
        throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: missing or invalid precedingOffset");
    }
    precedingOffset = config["precedingOffset"].get<long>();
    if (precedingOffset <= 0) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction: precedingOffset must be positive, got " +
            std::to_string(precedingOffset));
    }

    if (!config.contains("inputTypes") || !config["inputTypes"].is_array() || config["inputTypes"].empty()) {
        throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: missing or empty inputTypes");
    }
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();

    if (!config.contains("outputTypes") || !config["outputTypes"].is_array()) {
        throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: missing outputTypes");
    }
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();

    // grouping may be an empty array (OVER () = single global partition).
    if (!config.contains("grouping") || !config["grouping"].is_array()) {
        throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: missing or invalid grouping");
    }
    groupingIndices = config["grouping"].get<std::vector<int>>();

    if (!config.contains("aggInfoList") || !config["aggInfoList"].contains("aggregateCalls") ||
        !config["aggInfoList"]["aggregateCalls"].is_array() ||
        config["aggInfoList"]["aggregateCalls"].empty()) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction: missing or empty aggInfoList.aggregateCalls");
    }
    aggInfoList = config["aggInfoList"];
    accTypes = aggInfoList["accTypes"].get<std::vector<std::string>>();
    aggValueTypes = aggInfoList["aggValueTypes"].get<std::vector<std::string>>();

    // Over output row = (all input columns || one column per aggregate value).
    if (outputTypes.size() != inputTypes.size() + aggValueTypes.size()) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction: outputTypes arity mismatch (expected input + aggValueTypes)");
    }

    // Whitelist check happens here as the last line of defence; the real gate
    // is the OmniAdaptor Validate strategy, which keeps non-whitelisted plans
    // on the Java path so this throw is never reached in production.
    for (const auto& aggCall : aggInfoList["aggregateCalls"]) {
        const std::string name = aggCall["name"].get<std::string>();
        const std::string aggType = extractAggName(name);
        if (aggType == "NONE") {
            throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: unsupported aggregate: " + name);
        }
    }
}

void ProcTimeRowsBoundedPrecedingFunction::initAggFunctions(int& accStartingIndex, int& aggValueIndex)
{
    functions.clear();

    std::vector<OverAggCallMeta> metas;
    const auto& aggregateCalls = aggInfoList["aggregateCalls"];
    metas.reserve(aggregateCalls.size());
    for (size_t i = 0; i < aggregateCalls.size(); ++i) {
        const auto& aggCall = aggregateCalls[i];
        if (!aggCall.contains("name") || !aggCall["name"].is_string()) {
            throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: aggregateCalls missing 'name'");
        }
        if (!aggCall.contains("filterArg") || !aggCall["filterArg"].is_number_integer()) {
            throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: aggregateCalls missing 'filterArg'");
        }
        if (!aggCall.contains("argIndexes") || !aggCall["argIndexes"].is_array()) {
            throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: aggregateCalls missing 'argIndexes'");
        }

        OverAggCallMeta meta;
        meta.aggFuncIndex = static_cast<int>(i);
        meta.aggTypeStr = aggCall["name"].get<std::string>();
        meta.aggType = extractAggName(meta.aggTypeStr);
        meta.filterIndex = aggCall["filterArg"].get<int>();
        meta.argIndexes = aggCall["argIndexes"].get<std::vector<int>>();
        meta.aggIndex = meta.argIndexes.empty() ? -1 : meta.argIndexes[0];
        // Same range contract as the grouping indices in the constructor: an
        // argIndex outside inputTypes must fail construction instead of
        // indexing out of bounds below.
        if (meta.aggIndex != -1 &&
            (meta.aggIndex < 0 || meta.aggIndex >= static_cast<int>(inputTypes.size()))) {
            throw std::runtime_error(
                "ProcTimeRowsBoundedPrecedingFunction: aggregate arg index out of range: " +
                std::to_string(meta.aggIndex));
        }

        if (meta.aggType == "COUNT") {
            if (meta.argIndexes.size() > 1) {
                throw std::runtime_error("ProcTimeRowsBoundedPrecedingFunction: COUNT takes at most one argument");
            }
        } else { // SUM / MAX / MIN
            if (meta.argIndexes.size() != 1) {
                throw std::runtime_error(
                    "ProcTimeRowsBoundedPrecedingFunction: " + meta.aggType + " requires exactly one argument");
            }
        }
        meta.aggDataType = meta.aggIndex == -1 ? "BIGINT" : inputTypes[meta.aggIndex];
        metas.emplace_back(std::move(meta));
    }

    for (const auto& meta : metas) {
        AggsHandleFunction* function = nullptr;
        if (meta.aggType == "COUNT") {
            function = new CountFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
        } else if (meta.aggType == "MAX") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MAX_FUNC, meta.filterIndex);
        } else if (meta.aggType == "MIN") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MIN_FUNC, meta.filterIndex);
        } else if (meta.aggType == "SUM") {
            function = new SumFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
        } else {
            throw std::runtime_error(
                "ProcTimeRowsBoundedPrecedingFunction: unsupported aggregate: " + meta.aggTypeStr);
        }
        functions.push_back(function);
    }

    // Slot assignment (mirrors GroupAggFunction::InitAggFunctions): every
    // function occupies accumulatorSlots() slots in the shared accumulator row
    // and, when it produces an output value, one slot in the value row.
    int nextAccIndex = accStartingIndex;
    int nextValueIndex = aggValueIndex;
    for (const auto& meta : metas) {
        AggsHandleFunction* function = functions[meta.aggFuncIndex];
        function->bindAccValueIndex(nextAccIndex, nextValueIndex);
        nextAccIndex += function->accumulatorSlots();
        if (function->hasAggOutput()) {
            nextValueIndex++;
        }
    }

    accStartingIndex = nextAccIndex;
    aggValueIndex = nextValueIndex;
}

void ProcTimeRowsBoundedPrecedingFunction::open(const Configuration& parameters)
{
    LOG("ProcTimeRowsBoundedPrecedingFunction open() running");

    auto* runtimeContext = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext());

    // ---- four states, names and layouts mirroring the Java operator ----
    omnistream::RowType accRowType(true, this->accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    auto* accDesc = new ValueStateDescriptor<RowData*>("accState", accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());
    accState = runtimeContext->getState<RowData*>(accDesc);

    auto* counterDesc = new ValueStateDescriptor<int64_t>("processedCountState", new LongSerializer());
    counterState = runtimeContext->getState<int64_t>(counterDesc);

    auto* smallestTsDesc = new ValueStateDescriptor<int64_t>("smallestTSState", new LongSerializer());
    smallestTsState = runtimeContext->getState<int64_t>(smallestTsDesc);

    // Java: MapStateDescriptor<Long, List<RowData>> inputState
    // ("inputState", LONG_TYPE_INFO, new ListTypeInfo<>(inputType));
    // The native backends have no BIGINT-key / row-list-value MapState
    // combination, so the timestamp key is wrapped into a one-column
    // BinaryRowData and the state uses the TopN "data-state" shape
    // MapState<RowData, List<RowData>> (ROW_BK + ROW_LIST_BK).
    omnistream::RowType inputRowType(true, this->inputTypes);
    auto* listSerializer = new BinaryRowDataListSerializer(
        static_cast<int>(inputTypes.size()), new RowDataSerializer(&inputRowType));
    omnistream::RowType tsRowType(false, std::vector<std::string>{"BIGINT"});
    auto* inputDesc = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
        "inputState", new RowDataSerializer(&tsRowType), listSerializer);
    inputDesc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
    inputState = runtimeContext->getMapState<RowData*, std::vector<RowData*>*>(inputDesc);

#ifdef WITH_OMNISTATESTORE
    if (dynamic_cast<BssValueState<RowData*, VoidNamespace, RowData*>*>(accState)) {
        this->backend = 1;
    } else
#endif
    if (dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(accState)) {
        this->backend = 2;
    }

    int accStartingIndex = 0;
    int aggValueIndex = 0;
    initAggFunctions(accStartingIndex, aggValueIndex);

    if (accStartingIndex != accumulatorArity) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction open: acc slot count " + std::to_string(accStartingIndex) +
            " != accTypes arity " + std::to_string(accumulatorArity));
    }
    if (aggValueIndex != aggValueArity) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction open: aggValue slot count " + std::to_string(aggValueIndex) +
            " != aggValueTypes arity " + std::to_string(aggValueArity));
    }

    resultRow = new JoinedRowData();
    reuseAggValue = BinaryRowData::createBinaryRowDataWithMem(aggValueArity);
}

void ProcTimeRowsBoundedPrecedingFunction::processRowForKey(RowData* input, RowData* key, long currentTime)
{
    // 1. fetch (or create) the key's accumulator and bind the aggregate slots
    RowData* accumulators = accState->value();
    if (accumulators == nullptr) {
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        for (auto* func : functions) {
            func->createAccumulators(static_cast<BinaryRowData*>(accumulators));
        }
        accState->update(accumulators);
    }
    for (auto* func : functions) {
        func->setAccumulators(accumulators);
        func->setCurrentGroupKey(key);
        func->setBackend(backend);
    }

    // 2. oldest ingestion time. Two markers share this state, mirroring the
    //    two Java values that must stay distinguishable:
    //      0  = never initialised (Java: smallestTsState.value() == null)
    //      -1 = every bucket drained (Java: Long.MAX_VALUE). INT64_MAX
    //           cannot be stored here: the native ValueState<int64_t>
    //           reserves numeric_limits<int64_t>::max() as its "absent"
    //           sentinel (HeapValueState.h value() maps it back to the
    //           default 0), which would silently re-arm this init and make
    //           the next eviction retract the wrong bucket. Real ingestion
    //           times are epoch millis, so a negative marker is unambiguous.
    int64_t smallestTs = smallestTsState->value();
    if (smallestTs == 0) {
        smallestTs = currentTime;
        smallestTsState->update(smallestTs);
    }

    // 3. buffered-row counter (Java: null -> 0; an empty state reads as 0).
    int64_t counter = counterState->value();

    // 4. window full -> retract the oldest row; otherwise grow the counter
    if (counter == precedingOffset) {
        BinaryRowData* smallestKey = makeTsKeyRow(smallestTs);
        std::vector<RowData*>* bucket = nullptr;
        auto bucketOpt = inputState->get(smallestKey);
        if (bucketOpt.has_value()) {
            bucket = *bucketOpt;
        }
        if (bucket != nullptr && !bucket->empty()) {
            RowData* retractRow = (*bucket)[0];
            for (auto* func : functions) {
                func->retract(retractRow);
                func->getAccumulators(static_cast<BinaryRowData*>(accumulators));
            }
            bucket->erase(bucket->begin());
            // The evicted row's buffered copy leaves the window: free it here,
            // exactly once. Java relies on GC for the buffered row object; in
            // C++ the heap backend stored our copy and the RocksDB get()
            // returned a fresh deserialised copy for us to own.
            delete retractRow;
        }
        // Java LOG.warns and skips the retraction when the bucket is already
        // gone (state TTL cleared it). v1 never TTL-clears state, so this is
        // purely defensive and keeps the Java semantics of "skip, do not fail".
        if (bucket != nullptr && !bucket->empty()) {
            inputState->put(smallestKey, bucket); // key exists: value update only
            // Heap: the state map keeps storing this very vector pointer, so
            // it stays alive until the last row is evicted. RocksDB: put()
            // serialised it into the backend and retains nothing - free our copy.
            if (backend == 2) {
                for (auto* row : *bucket) {
                    delete row;
                }
                delete bucket;
            }
        } else {
            // Heap remove() only erases the map entry: it orphans both the
            // stored key row and the vector (Java's GC reclaims both; v1
            // accepts the bounded one-key-row-per-drain leak). The RocksDB
            // get() allocated the vector fresh for us. Either way the vector
            // is ours to free.
            inputState->remove(smallestKey);
            if (bucket != nullptr) {
                for (auto* row : *bucket) {
                    delete row;
                }
                delete bucket;
            }
            // Min-finding seed, exactly Java's newSmallestTs = Long.MAX_VALUE.
            // Real bucket keys are epoch millis, always smaller than this.
            int64_t newSmallestTs = INT64_MAX;
            // HeapMapState::entries() returns nullptr once the last entry was
            // removed (remove() clears the whole per-namespace map when it
            // empties). Java's inputState.keys() yields an empty iterator in
            // that case, so nullptr means "no buckets": keep the drained
            // marker.
            auto* allEntries = inputState->entries();
            if (allEntries != nullptr) {
                for (auto& entry : *allEntries) {
                    // entry.first is owned by the state; read the timestamp column.
                    int64_t ts = *entry.first->getLong(0);
                    if (ts < newSmallestTs) {
                        newSmallestTs = ts;
                    }
                }
            }
            // Java stores Long.MAX_VALUE when no bucket is left. The native
            // ValueState<int64_t> cannot hold INT64_MAX: value() treats it as
            // the "absent" sentinel and returns the default 0 (HeapValueState.h),
            // which would re-arm the == 0 init on the next row and evict the
            // wrong bucket (this exact failure produced {10,20,30,40} where
            // Java yields {10,20,50,70}). -1 is the drained marker instead.
            smallestTsState->update(newSmallestTs == INT64_MAX ? -1 : newSmallestTs);
        }
        // Temporary lookup key: no backend retains it (the heap map only
        // stores pointers for keys it did not have, and this key was present).
        delete smallestKey;
    } else {
        counter += 1;
        counterState->update(counter);
    }

    // 5. buffer the input row in its ingestion-time bucket. The bucket keeps
    //    its OWN copy of the row: Java buffers the input row object itself and
    //    lets the GC keep it alive, but the C++ collector frees the collected
    //    rows right after emission, so sharing the pointer would dangle the
    //    next time this bucket is retracted from. Key/vector ownership: the
    //    heap backend takes over the key row and the vector on the first put
    //    (they live until eviction); the RocksDB backend serialises both and
    //    retains nothing, so its temporary copies are freed right here.
    {
        BinaryRowData* tsKey = makeTsKeyRow(currentTime);
        auto bucketOpt = inputState->get(tsKey);
        if (bucketOpt.has_value()) {
            std::vector<RowData*>* bucket = *bucketOpt;
            bucket->push_back(input->copy());
            inputState->put(tsKey, bucket);
            delete tsKey;
            if (backend == 2) {
                for (auto* row : *bucket) {
                    delete row;
                }
                delete bucket;
            }
        } else {
            auto* bucket = new std::vector<RowData*>();
            bucket->push_back(input->copy());
            inputState->put(tsKey, bucket);
            if (backend == 2) {
                for (auto* row : *bucket) {
                    delete row;
                }
                delete bucket;
                delete tsKey;
            }
        }
    }

    // 6. accumulate the row into every aggregate function and write back
    for (auto* func : functions) {
        func->accumulate(input);
        func->getAccumulators(static_cast<BinaryRowData*>(accumulators));
    }
    accState->update(accumulators);

    // 7. snapshot the aggregate values and queue (input || aggValue) as INSERT
    materialiseAggValue();
    collectedRows.push_back(input);
    collectedAggValues.push_back(reuseAggValue->copy());
    collectedRowKinds.push_back(RowKind::INSERT);
}

void ProcTimeRowsBoundedPrecedingFunction::materialiseAggValue()
{
    for (auto* func : functions) {
        if (func->hasAggOutput()) {
            func->getValue(reuseAggValue);
        }
    }
}

BinaryRowData* ProcTimeRowsBoundedPrecedingFunction::makeTsKeyRow(int64_t ts) const
{
    // inputState keys are one-column BIGINT rows holding the ingestion time
    // (the wrapped Java Long key, see the class comment).
    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(1);
    key->setLong(0, ts);
    return key;
}

int64_t ProcTimeRowsBoundedPrecedingFunction::currentProcessingTimeMillis()
{
    // See the header comment: ContextImpl::timerService() is never wired in
    // the C++ KeyedProcessOperator, so the operator reads the wall clock
    // directly (the same approach as SliceAssigners). Java's
    // currentProcessingTime is likewise "now" at element processing.
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

RowData* ProcTimeRowsBoundedPrecedingFunction::materialiseRow(omnistream::VectorBatch* batch, int rowId)
{
    // Same pattern as ProcTimeUnboundedPrecedingFunction: materialise one
    // owned BinaryRowData from the columnar batch, dispatching on the JSON
    // inputTypes (the same type vocabulary every validation layer checked).
    int colsCount = batch->GetVectorCount();
    // The batch must carry exactly the described input columns: a mismatch
    // would both read inputTypes out of bounds here and shift the
    // input||aggValue column boundary on the output side.
    if (colsCount != static_cast<int>(inputTypes.size())) {
        throw std::runtime_error(
            "ProcTimeRowsBoundedPrecedingFunction: batch column count " + std::to_string(colsCount) +
            " != described inputTypes arity " + std::to_string(inputTypes.size()));
    }
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(colsCount);
    for (int32_t i = 0; i < colsCount; i++) {
        const std::string& typeStr = inputTypes[i];
        if (typeStr == "BIGINT" || typeStr.find("TIMESTAMP") != std::string::npos) {
            row->setLong(
                i,
                reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "INTEGER") {
            row->setInt(
                i, reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "DOUBLE") {
            // Native 8-byte IEEE754 slot (Java BinaryRowData equivalent):
            // setDouble keeps the exact bits; an arithmetic setLong would
            // silently truncate 3.14 to 3.
            row->setDouble(
                i, reinterpret_cast<omniruntime::vec::Vector<double>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "BOOLEAN") {
            row->setInt(
                i, reinterpret_cast<omniruntime::vec::Vector<bool>*>(batch->Get(i))->GetValue(rowId));
        } else if (
            typeStr == "STRING" || typeStr == "VARCHAR" ||
            typeStr == "VARCHAR(2147483647)") {
            auto str = reinterpret_cast<
                omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(i))
                              ->GetValue(rowId);
            auto u32Str = std::make_unique<std::u32string>(str.begin(), str.end());
            row->setString(i, new BinaryStringData(u32Str.get()));
        } else {
            delete row;
            throw std::runtime_error(
                "ProcTimeRowsBoundedPrecedingFunction: unsupported input column type: " + typeStr);
        }
    }
    row->setRowKind(batch->getRowKind(rowId));
    return row;
}

void ProcTimeRowsBoundedPrecedingFunction::processElement(
    RowData* input, Context* ctx, TimestampedCollector* out)
{
    // Single-row entry point (unit tests). The production path is processBatch.
    long currentTime = currentProcessingTimeMillis();
    RowData* owned = input->copy();
    processRowForKey(owned, ctx->getCurrentKey(), currentTime);
    flushCollected(*out);
}

void ProcTimeRowsBoundedPrecedingFunction::processBatch(
    omnistream::VectorBatch* inputBatch,
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    auto rowCount = inputBatch->GetRowCount();
    if (rowCount < 0) {
        delete inputBatch;
        return;
    }

    // v1: row-at-a-time over the batch. Rows are processed in arrival order;
    // the processing time is re-read per row like Java's processElement.
    for (int i = 0; i < rowCount; ++i) {
        long currentTime = currentProcessingTimeMillis();
        RowData* key = groupByKeySelector->getKey(inputBatch, i);
        ctx.setCurrentKey(key);
        RowData* row = materialiseRow(inputBatch, i);
        processRowForKey(row, key, currentTime);
        delete key;
    }

    flushCollected(out);
    delete inputBatch;
}

void ProcTimeRowsBoundedPrecedingFunction::flushCollected(TimestampedCollector& out)
{
    if (collectedRows.empty()) {
        return;
    }
    omnistream::VectorBatch* outputBatch = createOutputBatch();
    collectOutputBatch(out, outputBatch);
}

omnistream::VectorBatch* ProcTimeRowsBoundedPrecedingFunction::createOutputBatch()
{
    int numRows = static_cast<int>(collectedRows.size());
    int inputArity = collectedRows[0]->getArity();
    auto* outputBatch = new omnistream::VectorBatch(numRows);

    for (int colIndex = 0; colIndex < static_cast<int>(outputTypes.size()); ++colIndex) {
        // Output row = (input row || aggValue snapshot): the first inputArity
        // columns are read from the collected input rows, the rest from the
        // per-row aggregate-value snapshots.
        bool isValueCol = colIndex >= inputArity;
        int srcCol = isValueCol ? colIndex - inputArity : colIndex;
        std::vector<RowData*>& src = isValueCol ? collectedAggValues : collectedRows;

        switch (LogicalType::flinkTypeToOmniTypeId(outputTypes[colIndex])) {
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getLong(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_INT: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getInt(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                // The vector type must match the declared DOUBLE column: an
                // int64 vector here would make every downstream reader
                // reinterpret the IEEE754 bits (or worse). Values are read
                // back through the row's native double slot.
                auto* vector = new omniruntime::vec::Vector<double>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getDouble(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getInt(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                using VarcharVector =
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
                auto* vector = new VarcharVector(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        auto value = std::string(src[rowIndex]->getStringView(srcCol));
                        omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(
                            vector, rowIndex, (void*)&value);
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            default: {
                delete outputBatch;
                throw std::runtime_error(
                    "ProcTimeRowsBoundedPrecedingFunction: unsupported output column type: " +
                    outputTypes[colIndex]);
            }
        }
    }

    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRowKinds[rowIndex]);
    }

    for (auto* row : collectedRows) {
        delete row;
    }
    collectedRows.clear();
    for (auto* value : collectedAggValues) {
        delete value;
    }
    collectedAggValues.clear();
    collectedRowKinds.clear();
    return outputBatch;
}

void ProcTimeRowsBoundedPrecedingFunction::collectOutputBatch(
    TimestampedCollector& out, omnistream::VectorBatch* outputBatch)
{
    out.collect(outputBatch);
}

JoinedRowData* ProcTimeRowsBoundedPrecedingFunction::getResultRow()
{
    return resultRow;
}

ValueState<RowData*>* ProcTimeRowsBoundedPrecedingFunction::getValueState()
{
    return accState;
}

void ProcTimeRowsBoundedPrecedingFunction::close()
{
}
