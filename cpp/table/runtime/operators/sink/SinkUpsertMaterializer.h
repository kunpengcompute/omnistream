/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_SINKUPSERTMATERIALIZER_H
#define OMNISTREAM_SINKUPSERTMATERIALIZER_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

// ValueState.h uses RowData / Object: include RowData.h before ValueState.h
#include "table/data/RowData.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/RowKind.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/RowType.h"
#include "vector/vector_helper.h"

/**
 * Materializes a retract changelog into an upsert changelog before the sink.
 *
 * Flink: org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer
 * - keyed by the sink primary key (description JSON "grouping");
 * - state: ValueState<List<RowData>> (state name "values"; the row objects are
 *   stored directly in the state list, like Flink's ListSerializer(RowDataSerializer));
 * - +I/+U appends the row (or replaces the stored row with the same upsert key when
 *   "inputUpsertKey" is present): the first row of a key emits +I, otherwise +U;
 * - -U/-D removes the first stored row equal on the compare columns (ignoring
 *   RowKind): removing the last row emits the retract input row as -D, removing
 *   the latest row additionally re-emits the new latest row as +U, removing a
 *   middle row emits nothing;
 * - a retract that finds no equal row is logged and swallowed (Flink 1.16.3
 *   behavior for state cleared by TTL);
 * - output rows keep the input schema, only the RowKind changes.
 *
 * Both HashMapStateBackend and RocksDB (EmbeddedRocksDBStateBackend) are
 * supported. Rows of one batch are processed against a per-key working set:
 * the first touch of a key loads its list from the state, later rows of the same
 * key reuse the working list (duplicate key objects are released), and all
 * modifications are written back at the END of the batch. HEAP relies on
 * in-place visibility and only hands freshly created lists to the state;
 * RocksDB serializes every changed list back into the store (batch-end
 * write-back). On RocksDB without the falcon value-state cache the loaded
 * copies (list and rows) are released after the write-back; with the cache
 * enabled the cached objects stay owned by the cache. Replaced/removed rows
 * and retract input rows are freed after the output batch is assembled. The
 * (possibly empty) list stays in state after a removal-to-empty, which is
 * observably equivalent to Flink's state.clear() (the next accumulate of the
 * same key emits +I again).
 *
 * TTL is not implemented; savepoints are marked unsupported by the factory.
 */
template <typename K>
class SinkUpsertMaterializer : public KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*> {
public:
    explicit SinkUpsertMaterializer(const nlohmann::json& config);
    ~SinkUpsertMaterializer() override = default;

    void open(const Configuration& context) override;

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx,
        TimestampedCollector& out) override;

    void processElement(
        RowData* input,
        typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context* ctx,
        TimestampedCollector* out) override
    {
        NOT_IMPL_EXCEPTION;
    }

    JoinedRowData* getResultRow() override
    {
        return nullptr;
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    static bool isAccumulateMsg(RowKind kind)
    {
        return kind == RowKind::INSERT || kind == RowKind::UPDATE_AFTER;
    }

    // Per-key working set of the current batch. HEAP: list is the state's live
    // vector (in-place mutations are visible); RocksDB: list is a deserialized
    // copy we own (without falcon cache) that must be written back and freed.
    // list == nullptr until the key's first accumulate needs a list.
    struct WorkItem {
        std::vector<RowData*>* list = nullptr;
        bool fresh = false;   // list created in this batch, not yet handed to the state
        bool changed = false; // list modified in this batch (RocksDB write-back condition)
    };

    bool rocksBackend() const
    {
        return stateType_ == omnistream::StateType::ROCKSDB;
    }

    // Flink equalsIgnoreRowKind: compare the given columns (upsert-key projection
    // when present, otherwise the whole row), two nulls compare equal.
    bool rowsEqual(RowData* a, RowData* b)
    {
        for (auto col : compareIndices_) {
            bool null1 = a->isNullAt(col);
            bool null2 = b->isNullAt(col);
            if (null1 != null2) {
                return false;
            }
            if (null1) {
                continue;
            }
            switch (inputTypeIds_[col]) {
                case omniruntime::type::DataTypeId::OMNI_INT: {
                    if (*a->getInt(col) != *b->getInt(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    if (*a->getLong(col) != *b->getLong(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    if (*a->getDouble(col) != *b->getDouble(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    if (*a->getBool(col) != *b->getBool(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    if (a->getStringView(col) != b->getStringView(col)) {
                        return false;
                    }
                    break;
                }
                default:
                    THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: unsupported compare column type");
            }
        }
        return true;
    }

    // Flink findFirst: index of the first stored row equal to the target, -1 if none.
    int findEqualRow(const std::vector<RowData*>& values, RowData* target)
    {
        for (size_t idx = 0; idx < values.size(); idx++) {
            if (rowsEqual(values[idx], target)) {
                return static_cast<int>(idx);
            }
        }
        return -1;
    }

    // Flink addRow: appends (or replaces the row with the same upsert key); the
    // row itself is emitted with the kind decided by the pre-add list state.
    void processAccumulate(WorkItem& item, RowData* row, int64_t timestamp)
    {
        if (item.list == nullptr) {
            item.list = new std::vector<RowData*>();
            item.fresh = true;
        }
        auto* values = item.list;
        RowKind outKind = values->empty() ? RowKind::INSERT : RowKind::UPDATE_AFTER;
        if (hasUpsertKey_) {
            int index = findEqualRow(*values, row);
            if (index >= 0) {
                deleteAfterOutput_.push_back((*values)[index]);
                (*values)[index] = row;
            } else {
                values->push_back(row);
            }
        } else {
            values->push_back(row);
        }
        item.changed = true;
        emit(row, outKind, timestamp);
    }

    // Flink retractRow: removes the first equal row; emits the retract input row
    // as -D when the list becomes empty, re-emits the new latest row as +U when
    // the latest row was removed and rows remain.
    void processRetract(WorkItem& item, RowData* row, int64_t timestamp)
    {
        if (item.list == nullptr || item.list->empty()) {
            WARN_RELEASE("SinkUpsertMaterializer: retract matches no stored row (state cleared?); swallowed");
            deleteAfterOutput_.push_back(row);
            return;
        }
        auto* values = item.list;
        int index = findEqualRow(*values, row);
        if (index < 0) {
            WARN_RELEASE("SinkUpsertMaterializer: retract matches no stored row (state cleared?); swallowed");
            deleteAfterOutput_.push_back(row);
            return;
        }
        int lastIndexBeforeRemove = static_cast<int>(values->size()) - 1;
        RowData* removed = (*values)[index];
        values->erase(values->begin() + index);
        deleteAfterOutput_.push_back(removed);
        item.changed = true;
        if (values->empty()) {
            emit(row, RowKind::DELETE, timestamp);
        } else if (index == lastIndexBeforeRemove) {
            // Removed the latest row: re-emit the new latest row as +U (Flink retractRow).
            emit(values->back(), RowKind::UPDATE_AFTER, timestamp);
            deleteAfterOutput_.push_back(row);
        } else {
            // Removing a middle row emits nothing. The (possibly empty) list stays in
            // state so the next accumulate of the same key emits +I again.
            deleteAfterOutput_.push_back(row);
        }
    }

    // Batch-end write-back. HEAP: only freshly created lists are handed to the
    // state (in-place mutations of existing lists are already visible). RocksDB:
    // every changed list is serialized back; without the falcon cache the loaded
    // copies (list and rows) are freed afterwards - their bytes now live in the
    // store. Retained key objects are freed by the caller after this loop.
    void flushWorkItems(
        typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx)
    {
        for (RowData* key : retainedKeys_) {
            WorkItem& item = workItems_[key];
            if (item.list == nullptr) {
                continue;
            }
            if (item.fresh || (rocksBackend() && item.changed)) {
                ctx.setCurrentKey(key);
                state_->update(item.list);
            }
            if (rocksBackend() && !rocksdbCacheEnabled_) {
                for (RowData* storedRow : *item.list) {
                    delete storedRow;
                }
                delete item.list;
            }
        }
    }

    void emit(RowData* row, RowKind kind, int64_t timestamp)
    {
        row->setRowKind(kind);
        collectedRows_.push_back(row);
        collectedRowKinds_.push_back(kind);
        collectedTimestamps_.push_back(timestamp);
    }

    // Assembles the output batch from the collected rows: column values and nulls
    // are copied per column, only the RowKind is rewritten.
    omnistream::VectorBatch* buildOutput()
    {
        int numRows = static_cast<int>(collectedRows_.size());
        auto* outputVB = new omnistream::VectorBatch(numRows);
        for (size_t col = 0; col < inputTypeIds_.size(); col++) {
            switch (inputTypeIds_[col]) {
                case omniruntime::type::DataTypeId::OMNI_INT: {
                    auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                        RowData* row = collectedRows_[rowIndex];
                        if (row->isNullAt(col)) {
                            vector->SetNull(rowIndex);
                        } else {
                            vector->SetValue(rowIndex, *row->getInt(col));
                        }
                    }
                    outputVB->Append(vector);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                        RowData* row = collectedRows_[rowIndex];
                        if (row->isNullAt(col)) {
                            vector->SetNull(rowIndex);
                        } else {
                            vector->SetValue(rowIndex, *row->getLong(col));
                        }
                    }
                    outputVB->Append(vector);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    auto* vector = new omniruntime::vec::Vector<double>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                        RowData* row = collectedRows_[rowIndex];
                        if (row->isNullAt(col)) {
                            vector->SetNull(rowIndex);
                        } else {
                            vector->SetValue(rowIndex, *row->getDouble(col));
                        }
                    }
                    outputVB->Append(vector);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                        RowData* row = collectedRows_[rowIndex];
                        if (row->isNullAt(col)) {
                            vector->SetNull(rowIndex);
                        } else {
                            vector->SetValue(rowIndex, *row->getBool(col));
                        }
                    }
                    outputVB->Append(vector);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    auto* vector =
                        new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                            numRows);
                    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                        RowData* row = collectedRows_[rowIndex];
                        if (row->isNullAt(col)) {
                            vector->SetNull(rowIndex);
                        } else {
                            auto value = std::string(row->getStringView(col));
                            omniruntime::vec::VectorHelper::VectorSetValue<omniruntime::type::DataTypeId::OMNI_VARCHAR>(
                                vector, rowIndex, (void*)&value);
                        }
                    }
                    outputVB->Append(vector);
                    break;
                }
                default: THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: unsupported output column type");
            }
        }
        if (numRows > 0) {
            outputVB->setTimestamps(0, collectedTimestamps_.data(), numRows);
            outputVB->setRowKinds(0, collectedRowKinds_.data(), numRows);
        }
        return outputVB;
    }

    void cleanup()
    {
        collectedRows_.clear();
        collectedRowKinds_.clear();
        collectedTimestamps_.clear();
        deleteAfterOutput_.clear();
        // Retained key objects are owned by this operator (KeySelector hands out
        // fresh objects); freed here after the flush used them.
        for (RowData* key : retainedKeys_) {
            delete key;
        }
        retainedKeys_.clear();
        workItems_.clear();
    }

    std::vector<std::string> inputTypes_;
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;
    std::vector<int32_t> groupingIndices_;
    std::vector<int32_t> inputUpsertKeyIndices_;
    bool hasUpsertKey_ = false;
    std::vector<int32_t> compareIndices_;
    std::unique_ptr<KeySelector<K>> keySelector_ = nullptr;

    ValueState<std::vector<RowData*>*>* state_ = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
    bool rocksdbCacheEnabled_ = false;

    std::unordered_map<RowData*, WorkItem> workItems_{};
    std::vector<RowData*> retainedKeys_{};
    std::vector<RowData*> collectedRows_{};
    std::vector<RowKind> collectedRowKinds_{};
    std::vector<int64_t> collectedTimestamps_{};
    std::vector<RowData*> deleteAfterOutput_{};
};

template <typename K>
SinkUpsertMaterializer<K>::SinkUpsertMaterializer(const nlohmann::json& config)
{
    inputTypes_ = config["inputTypes"].get<std::vector<std::string>>();
    if (config.contains("grouping") && !config["grouping"].is_null()) {
        groupingIndices_ = config["grouping"].get<std::vector<int32_t>>();
    }
    if (groupingIndices_.empty()) {
        THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: 'grouping' (sink primary key indices) must not be empty");
    }
    if (config.contains("inputUpsertKey") && !config["inputUpsertKey"].is_null()) {
        inputUpsertKeyIndices_ = config["inputUpsertKey"].get<std::vector<int32_t>>();
    }
    hasUpsertKey_ = !inputUpsertKeyIndices_.empty();

    for (auto& typeStr : inputTypes_) {
        auto typeId = LogicalType::flinkTypeToOmniTypeId(typeStr);
        switch (typeId) {
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                inputTypeIds_.push_back(typeId);
                break;
            default:
                THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: unsupported input column type '" + typeStr + "'");
        }
    }
    if (hasUpsertKey_) {
        compareIndices_ = inputUpsertKeyIndices_;
    } else {
        compareIndices_.resize(inputTypeIds_.size());
        for (size_t i = 0; i < inputTypeIds_.size(); i++) {
            compareIndices_[i] = static_cast<int32_t>(i);
        }
    }

    std::vector<int32_t> keyTypeIds;
    keyTypeIds.reserve(groupingIndices_.size());
    for (auto idx : groupingIndices_) {
        if (idx < 0 || static_cast<size_t>(idx) >= inputTypeIds_.size()) {
            THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: grouping index out of range");
        }
        keyTypeIds.push_back(inputTypeIds_[idx]);
    }
    keySelector_ = std::make_unique<KeySelector<K>>(keyTypeIds, groupingIndices_);
}

template <typename K>
void SinkUpsertMaterializer<K>::open(const Configuration& context)
{
    auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
    stateType_ = runtimeContext->getStateType();
    if (stateType_ != omnistream::StateType::HEAP && stateType_ != omnistream::StateType::ROCKSDB) {
        THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer only supports heap and RocksDB state backends");
    }
    // Flink: ValueStateDescriptor<List<RowData>>("values", new ListSerializer<>(serializer))
    // - rows are stored directly in the state list (no comboId / side tables).
    int inputArity = static_cast<int>(inputTypeIds_.size());
    auto* inputType = new omnistream::RowType(false, inputTypes_);
    auto* rowListSerializer = new BinaryRowDataListSerializer(inputArity, new RowDataSerializer(inputType));
    auto* stateDescriptor = new ValueStateDescriptor<std::vector<RowData*>*>("values", rowListSerializer);
    state_ = runtimeContext->template getState<std::vector<RowData*>*>(stateDescriptor);
    if (rocksBackend()) {
        // With the falcon value-state cache the loaded lists stay owned by the
        // cache (value() hands out cached objects); without it they are plain
        // deserialized copies we must free after the batch-end write-back.
        auto* rocksdbState =
            dynamic_cast<RocksdbValueState<K, VoidNamespace, std::vector<RowData*>*>*>(state_);
        if (rocksdbState == nullptr) {
            THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer failed to resolve RocksDB ValueState");
        }
        rocksdbCacheEnabled_ = rocksdbState->stateCache->getSizeLimit() != 0;
    }
}

template <typename K>
void SinkUpsertMaterializer<K>::processBatch(
    omnistream::VectorBatch* inputBatch,
    typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx,
    TimestampedCollector& out)
{
    auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
    int rowCnt = inputBatchGuard->GetRowCount();
    cleanup();

    // Row-by-row materialization against a per-key working set: the first touch
    // of a key switches the key context and loads the key's list from the state,
    // later rows of the same key reuse the working list (duplicate key objects
    // handed out by the KeySelector are released). Each input row is extracted
    // into an independent BinaryRowData (we take ownership); accumulate stores it
    // into the working list, retract uses it for the equality match. Emissions
    // keep the input schema and only rewrite the RowKind.
    for (int i = 0; i < rowCnt; i++) {
        RowData* rawKey = keySelector_->getKey(inputBatchGuard.get(), i);
        auto it = workItems_.find(rawKey);
        if (it == workItems_.end()) {
            ctx.setCurrentKey(rawKey);
            auto* list = state_->value();
            auto [inserted, ok] = workItems_.emplace(rawKey, WorkItem{list, false, false});
            retainedKeys_.push_back(rawKey);
            it = inserted;
        } else {
            delete rawKey;
        }
        WorkItem& item = it->second;
        RowData* row = inputBatchGuard->extractRowData(i);
        int64_t timestamp = inputBatchGuard->getTimestamp(i);
        if (isAccumulateMsg(row->getRowKind())) {
            processAccumulate(item, row, timestamp);
        } else {
            processRetract(item, row, timestamp);
        }
    }

    auto* outputVB = buildOutput();
    out.collect(outputVB);

    // Batch-end write-back (and release of RocksDB copies), then free the rows
    // replaced or removed from the state and the retract input rows that were
    // only used for the comparison - after the output batch is assembled.
    flushWorkItems(ctx);
    for (RowData* row : deleteAfterOutput_) {
        delete row;
    }
    cleanup();
}

#endif // OMNISTREAM_SINKUPSERTMATERIALIZER_H
