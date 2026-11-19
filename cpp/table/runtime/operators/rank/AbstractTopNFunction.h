/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_ABSTRACTTOPNFUNCTION_H
#define FLINK_TNEL_ABSTRACTTOPNFUNCTION_H

#include <cstring>
#include <cstdint>
#include <unordered_map>
#include "runtime/state/heap/HeapValueState.h"
#include "core/api/common/state/ValueState.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "vector/vector_helper.h"
#include <vector>
#include <functional>
#include <memory>
#include <iostream>
#include <regex>

#include "rank_range.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/RowKind.h"
#include "data/binary/BinaryRowData.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "functions/OpenContext.h"
#include "TopNBuffer.h"
#include "SetTopNBuffer.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
using namespace omnistream;

// Encapsulates Flink's nullable ValueState<Long>("rankEnd") used by the variable
// rank range. Presence is determined by state-entry existence (ValueState::isEmpty),
// not by a numeric sentinel, so a frozen 0 or LONG_MAX is preserved exactly like
// Flink's boxed Long (null == not set). The value is still persisted with
// LongSerializer, keeping checkpoint bytes identical to Flink.
//
// Shared by all TopN variants (Retractable now, Updatable/AppendOnly later): they
// only call open()/getFrozenValue()/freeze(), never the raw state.
class RankEndState {
public:
    RankEndState() = default;

    template <typename K>
    void open(StreamingRuntimeContext<K>* runtimeContext)
    {
        auto* rankEndStateDescriptor = new ValueStateDescriptor<long>("rankEnd", LongSerializer::INSTANCE);
        state_ = runtimeContext->template getState<long>(rankEndStateDescriptor);
    }

    bool isRegistered() const
    {
        return state_ != nullptr;
    }

    // Reads the frozen rank end. Returns false only when no value has been frozen
    // for the current key (Flink: rankEndState.value() == null).
    bool getFrozenValue(long& out)
    {
        return state_ != nullptr && state_->getIfPresent(out);
    }

    void freeze(long value)
    {
        state_->update(value);
    }

private:
    ValueState<long>* state_ = nullptr;
};

template <typename KeyType>
class AbstractTopNFunction : public KeyedProcessFunction<KeyType, RowData*, RowData*> {
public:
    explicit AbstractTopNFunction(const nlohmann::json& rankConfig)
        : inputTypeNames(new std::vector<std::string>()),
          inputRowType(new std::vector<omniruntime::type::DataTypeId>())
    {
        parseDescription(rankConfig);
    }

    virtual void open(const Configuration& context) = 0;
    void parseDescription(const nlohmann::json& jsonString);

    void processElement(
        RowData* input,
        typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context* ctx,
        TimestampedCollector* out) override {};
    ~AbstractTopNFunction()
    {
        delete rankRange;
        for (auto row : collectedRows) {
            if (row != nullptr) {
                delete row;
            }
        }
        delete inputTypeNames;
        delete inputRowType;
    };

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override {};
    JoinedRowData* getResultRow() override
    {
        return nullptr;
    };

    omnistream::VectorBatch* createOutputBatch();

protected:
    std::string processFunction;
    RankRange* rankRange;
    bool outputRankNumber;
    bool generateUpdateBefore;
    std::vector<std::string>* inputTypeNames;
    std::vector<omniruntime::type::DataTypeId>* inputRowType;
    std::vector<bool> sortNullsIsLast;
    std::vector<int> partitionKeyTypeIds;
    std::vector<int> partitionKeyIndices;
    std::vector<int> sortKeyIndices;
    std::vector<omniruntime::type::DataTypeId> sortKeyTypeIds;
    std::vector<bool> sortOrder;
    std::vector<RowData*> collectedRows;
    std::vector<int64_t> collectedTimestamps;
    std::vector<int64_t> collectedRanks;
    std::vector<RowKind> collectedRowKinds;

    long hitCount = 0L;
    long requestCount = 0L;
    void collectInsert(RowData* inputRow, long rank, int64_t timestamp);
    void collectInsert(RowData* inputRow, int64_t timestamp);
    void collectInsert(RowData* inputRow);
    void collectDelete(RowData* inputRow, long rank, int64_t timestamp);
    void collectDelete(RowData* inputRow, int64_t timestamp);
    void collectDelete(RowData* inputRow);
    void collectUpdateBefore(RowData* inputRow, long rank, int64_t timestamp);
    void collectUpdateAfter(RowData* inputRow, long rank, int64_t timestamp);
    void collectOutputBatch(TimestampedCollector out, omnistream::VectorBatch* outputBatch);
    bool isInRankRange(long rank) const;
    inline bool isInRankEnd(long rank) const
    {
        return rank <= rankEnd;
    }
    long getDefaultTopNSize()
    {
        return isConstantRankEnd ? rankEnd : DEFAULT_TOPN_SIZE;
    }
    long initRankEnd(RowData* row)
    {
        if (isConstantRankEnd) {
            return rankEnd;
        }
        if (row == nullptr) {
            throw std::runtime_error(
                "variable rank end requires the input row to read the rank end column");
        }
        if (!rankEndState.isRegistered()) {
            throw std::runtime_error(
                "rankEnd state is not registered for variable rank end");
        }
        long curRankEnd = fetchRankEnd(row);
        // Presence mirrors Flink's boxed Long: a state entry (any value, including
        // 0 or LONG_MAX) means "frozen"; only an absent entry freezes on first touch.
        long stored = 0;
        if (!rankEndState.getFrozenValue(stored)) {
            // First record for this partition freezes the rank end (Flink's
            // initRankEnd: rankEndState.value() == null -> update with the current value).
            rankEnd = curRankEnd;
            rankEndState.freeze(rankEnd);
            return rankEnd;
        }
        // Subsequent records only read the frozen value; a different column value
        // just increments the invalid counter (Flink's topn.invalidTopSize).
        rankEnd = stored;
        if (rankEnd != curRankEnd) {
            ++invalidCount;
        }
        return rankEnd;
    }

    // Reads the variable rank end column. Flink only allows BIGINT/INTEGER/SMALLINT
    // (row.getLong/getInt/getShort); SMALLINT has no accessor in the native RowData,
    // so it is rejected like Flink's default error branch.
    long fetchRankEnd(RowData* row) const
    {
        const int idx = rankEndIndex;
        if (row->isNullAt(idx)) {
            // Flink's BinaryRowData getters do not null-check; a null field reads as 0.
            return 0L;
        }
        switch (inputRowType->at(static_cast<size_t>(idx))) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
                return *row->getLong(idx);
            case omniruntime::type::DataTypeId::OMNI_INT:
                return static_cast<long>(*row->getInt(idx));
            default:
                throw std::runtime_error(
                    "variable rank index column must be long, short or int type, while input type is " +
                    std::to_string(static_cast<int>(inputRowType->at(static_cast<size_t>(idx)))));
        }
    }
    bool hasOffset() const
    {
        // rank start is 1-based
        return rankStart > 1;
    }
    bool hasOutputRows() const
    {
        return !collectedRows.empty();
    }
    bool checkSortKeyInBufferRange(RowData* sortKey, TopNBuffer buffer)
    {
        return buffer.checkSortKeyInBufferRange(sortKey, getDefaultTopNSize());
    }

protected:
    bool isConstantRankEnd = true;
    int rankEndIndex;
    long rankStart;
    long rankEnd;
    static constexpr long DEFAULT_TOPN_SIZE = 100;

    // Per-key persisted rank end for the variable rank range ("rankEnd" state,
    // Flink's ValueStateDescriptor<Long>("rankEnd", Types.LONG)). Nullability is
    // modelled by state-entry presence (see RankEndState), so a frozen 0 and
    // LONG_MAX both round-trip; the raw value keeps Flink's LongSerializer bytes.
    // Registered by the derived function's open() when the rank range is variable;
    // unused for constant ranges.
    RankEndState rankEndState;
    // Counter of records whose rank-end column differs from the frozen value
    // (Flink's metric "topn.invalidTopSize"). Output is unaffected.
    long invalidCount = 0L;
};

template <typename KeyType>
void AbstractTopNFunction<KeyType>::parseDescription(const nlohmann::json& jsonString)
{
    // Parse partitionKey as an integer (assumes single value in array).
    // An empty array is legal: the streaming Limit shape uses PartitionSpec.ALL_IN_ONE
    // (single global partition, empty key row), so only a missing/non-array field throws.
    if (jsonString.contains("partitionKey") && jsonString["partitionKey"].is_array()) {
        partitionKeyIndices = jsonString["partitionKey"].get<std::vector<int>>();
    } else {
        throw std::runtime_error("Missing or invalid partitionKey");
    }

    // Parse outputRankNumber as a boolean
    outputRankNumber = jsonString.at("outputRankNumber").get<bool>();

    // Parse generateUpdateBefore as a boolean
    generateUpdateBefore = jsonString.at("generateUpdateBefore").get<bool>();

    // Parse processFunction as a string
    processFunction = jsonString.at("processFunction").get<std::string>();

    // Parse rankRange. A fixed range ("rankStart=1, rankEnd=3") maps to
    // ConstantRankRange. A variable range ("rankEnd=$<index>", the toString of
    // Flink's VariableRankRange) maps to VariableRankRange: the rank end is read
    // from a data column at runtime, mirroring Flink's AbstractTopNFunction.
    std::string rankRangeStr = jsonString.at("rankRange").get<std::string>();
    std::regex pattern(R"(rankStart=(\d+),\s*rankEnd=(\d+))"); // Regex pattern
    std::smatch match;
    if (std::regex_search(rankRangeStr, match, pattern)) {
        rankStart = std::stoi(match[1].str()); // Extract first number
        rankEnd = std::stoi(match[2].str());   // Extract second number
        rankRange = new ConstantRankRange(rankStart, rankEnd);
    } else {
        std::regex variablePattern(R"(rankEnd=\$(\d+))");
        std::smatch variableMatch;
        if (std::regex_search(rankRangeStr, variableMatch, variablePattern)) {
            rankEndIndex = std::stoi(variableMatch[1].str());
            isConstantRankEnd = false;
            rankStart = -1;
            rankEnd = -1;
            rankRange = new VariableRankRange(rankEndIndex);
        } else {
            throw std::runtime_error("Pattern not found!");
        }
    }

    // Parse inputTypes as a vector of DataTypeId
    for (const auto& typeStr : jsonString.at("inputTypes").get<std::vector<std::string>>()) {
        inputTypeNames->push_back(typeStr);
        inputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    for (const auto& col : partitionKeyIndices) {
        partitionKeyTypeIds.push_back(inputRowType->at(col));
    }

    // Parse sortFieldIndices as a vector of integers
    sortKeyIndices = jsonString.at("sortFieldIndices").get<std::vector<int>>();
    for (const auto& keyCol : sortKeyIndices) {
        sortKeyTypeIds.push_back(inputRowType->at(keyCol));
    }

    // Parse sortAscendingOrders as a vector of booleans
    sortOrder = jsonString.at("sortAscendingOrders").get<std::vector<bool>>();

    // Parse sortNullsIsLast as a vector of booleans
    sortNullsIsLast = jsonString.at("sortNullsIsLast").get<std::vector<bool>>();
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectInsert(RowData* inputRow, long rank, int64_t timestamp)
{
    if (isInRankRange(rank)) {
        collectedRows.push_back(inputRow);
        collectedRanks.push_back(rank);
        collectedTimestamps.push_back(timestamp);
        collectedRowKinds.push_back(RowKind::INSERT);
    }
}

// Without-row-number insertion: mirrors Flink's collectInsert(out, inputRow) which
// emits unconditionally. rank stays 0 (the rank column is only appended when
// outputRankNumber, which never holds on this path).
template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectInsert(RowData* inputRow, int64_t timestamp)
{
    collectedRows.push_back(inputRow);
    collectedRanks.push_back(0);
    collectedTimestamps.push_back(timestamp);
    collectedRowKinds.push_back(RowKind::INSERT);
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectInsert(RowData* inputRow)
{
    inputRow->setRowKind(RowKind::INSERT);
    collectedRows.push_back(inputRow);
    collectedRanks.push_back(0);
    collectedTimestamps.push_back(0);
    collectedRowKinds.push_back(RowKind::INSERT);
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectDelete(RowData* inputRow, long rank, int64_t timestamp)
{
    // Placeholder for delete logic
    LOG("rank " + std::to_string(rank));
    if (isInRankRange(rank)) {
        collectedRows.push_back(inputRow);
        collectedRanks.push_back(rank);
        collectedTimestamps.push_back(timestamp);
        collectedRowKinds.push_back(RowKind::DELETE);
    }
}

// Without-row-number deletion: mirrors Flink's collectDelete(out, inputRow) which
// emits unconditionally.
template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectDelete(RowData* inputRow, int64_t timestamp)
{
    collectedRows.push_back(inputRow);
    collectedRanks.push_back(0);
    collectedTimestamps.push_back(timestamp);
    collectedRowKinds.push_back(RowKind::DELETE);
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectDelete(RowData* inputRow)
{
    if (!inputRow) {
        LOG("input row in null");
        return;
    }
    inputRow->setRowKind(RowKind::DELETE);
    collectedRows.push_back(inputRow);
    collectedRanks.push_back(0);
    collectedTimestamps.push_back(0);
    collectedRowKinds.push_back(RowKind::DELETE);
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectUpdateBefore(RowData* inputRow, long rank, int64_t timestamp)
{
    LOG("rank " + std::to_string(rank));
    if (generateUpdateBefore && isInRankRange(rank)) {
        collectedRows.push_back(inputRow);
        collectedRanks.push_back(rank);
        collectedTimestamps.push_back(timestamp);
        collectedRowKinds.push_back(RowKind::UPDATE_BEFORE);
    }
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectUpdateAfter(RowData* inputRow, long rank, int64_t timestamp)
{
    LOG("rank " + std::to_string(rank));
    if (isInRankRange(rank)) {
        collectedRows.push_back(inputRow);
        collectedRanks.push_back(rank);
        collectedTimestamps.push_back(timestamp);
        collectedRowKinds.push_back(RowKind::UPDATE_AFTER);
    }
}

template <typename KeyType>
bool AbstractTopNFunction<KeyType>::isInRankRange(long rank) const
{
    return rank >= rankStart && rank <= rankEnd;
}

template <typename KeyType>
omnistream::VectorBatch* AbstractTopNFunction<KeyType>::createOutputBatch()
{
    int numColumns = inputRowType->size();
    int numRows = collectedRows.size(); // Number of rows collected

    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    //  Loop through each column and create vectors. Collected rows keep the
    // null bits of the source rows (extractRowData sets setNullAt), so every
    // value copy must check the null bit and call SetNull - a blind
    // getLong/getInt/getStringView dereference would silently emit 0/empty
    // (and for CHAR read stale fixed-slot bytes as a bogus string_view).
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (inputRowType->at(colIndex)) {
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_INT:
            case DataTypeId::OMNI_DATE32: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                auto* vector = new omniruntime::vec::Vector<double>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getDouble(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getBool(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_CHAR:
            case DataTypeId::OMNI_VARCHAR: {
                using VarcharVector =
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
                VarcharVector* vector = new VarcharVector(
                    numRows, omniruntime::vec::INITIAL_STRING_SIZE, inputRowType->at(colIndex));
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        auto value = std::string(collectedRows[rowIndex]->getStringView(colIndex));
                        omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(
                            vector, rowIndex, (void*)&value);
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            default: {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }
    if (outputRankNumber) {
        auto rankVec = new omniruntime::vec::Vector<int64_t>(numRows);
        rankVec->SetValues(0, collectedRanks.data(), numRows);
        outputBatch->Append(rankVec);
    }
    outputBatch->setTimestamps(0, collectedTimestamps.data(), numRows);
    outputBatch->setRowKinds(0, collectedRowKinds.data(), numRows);

    collectedRows.clear();
    collectedRowKinds.clear();
    collectedTimestamps.clear();
    collectedRanks.clear();
    return outputBatch;
}

template <typename KeyType>
void AbstractTopNFunction<KeyType>::collectOutputBatch(TimestampedCollector out, omnistream::VectorBatch* outputBatch)
{
    out.collect(outputBatch);
}

template class AbstractTopNFunction<long>;
template class AbstractTopNFunction<int>;
template class AbstractTopNFunction<double>;

#endif // FLINK_TNEL_ABSTRACTTOPNFUNCTION_H
