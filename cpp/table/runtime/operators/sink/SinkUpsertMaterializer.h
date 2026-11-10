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
#include <vector>

#include <nlohmann/json.hpp>

// ValueState.h uses RowData / Object: include RowData.h before ValueState.h
#include "table/data/RowData.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
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
 * Row ownership (HashMapStateBackend): rows appended into the state list are
 * owned by the state; rows replaced/removed from the list are deleted at batch
 * end; the retract input row is deleted once compared (and after the output
 * batch is assembled when it is emitted as -D). The (possibly empty) list stays
 * in state after a removal-to-empty, which is observably equivalent to Flink's
 * state.clear() (the next accumulate of the same key emits +I again).
 *
 * State backend: HashMapStateBackend only (fail-fast otherwise). TTL is not
 * implemented; savepoints are marked unsupported by the factory.
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
    void processAccumulate(RowData* row, int64_t timestamp)
    {
        auto* values = state_->value();
        bool existed = values != nullptr;
        if (!existed) {
            values = new std::vector<RowData*>();
        }
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
        if (!existed) {
            // Fresh key: hand the new vector over to the state. For an existing key
            // the in-place mutation of the stored vector is already visible (HEAP).
            state_->update(values);
        }
        emit(row, outKind, timestamp);
    }

    // Flink retractRow: removes the first equal row; emits the retract input row
    // as -D when the list becomes empty, re-emits the new latest row as +U when
    // the latest row was removed and rows remain.
    void processRetract(RowData* row, int64_t timestamp)
    {
        auto* values = state_->value();
        if (values == nullptr || values->empty()) {
            WARN_RELEASE("SinkUpsertMaterializer: retract matches no stored row (state cleared?); swallowed");
            deleteAfterOutput_.push_back(row);
            return;
        }
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
    if (stateType_ != omnistream::StateType::HEAP) {
        // RocksDB support needs the batched updateByBatch write pattern of
        // AppendOnlyTopNFunction plus ownership verification; tracked as follow-up.
        THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer currently only supports HashMapStateBackend");
    }
    // Flink: ValueStateDescriptor<List<RowData>>("values", new ListSerializer<>(serializer))
    // - rows are stored directly in the state list (no comboId / side tables).
    int inputArity = static_cast<int>(inputTypeIds_.size());
    auto* inputType = new omnistream::RowType(false, inputTypes_);
    auto* rowListSerializer = new BinaryRowDataListSerializer(inputArity, new RowDataSerializer(inputType));
    auto* stateDescriptor = new ValueStateDescriptor<std::vector<RowData*>*>("values", rowListSerializer);
    state_ = runtimeContext->template getState<std::vector<RowData*>*>(stateDescriptor);
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

    // Row-by-row materialization: each input row is extracted into an independent
    // BinaryRowData (we take ownership); accumulate stores it into the state list,
    // retract uses it for the equality match. Emissions keep the input schema and
    // only rewrite the RowKind.
    for (int i = 0; i < rowCnt; i++) {
        auto key = keySelector_->getKey(inputBatchGuard.get(), i);
        ctx.setCurrentKey(key);
        RowData* row = inputBatchGuard->extractRowData(i);
        int64_t timestamp = inputBatchGuard->getTimestamp(i);
        if (isAccumulateMsg(row->getRowKind())) {
            processAccumulate(row, timestamp);
        } else {
            processRetract(row, timestamp);
        }
    }

    auto* outputVB = buildOutput();
    out.collect(outputVB);

    // Rows replaced or removed from the state and retract input rows that were
    // only used for the comparison are freed after the output batch is assembled.
    for (RowData* row : deleteAfterOutput_) {
        delete row;
    }
    cleanup();
}

#endif // OMNISTREAM_SINKUPSERTMATERIALIZER_H
