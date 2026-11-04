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
#include "runtime/state/KeyGroupRangeAssignment.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/RowKind.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/SortedVectorLong.h"
#include "table/types/logical/LogicalType.h"

/**
 * Materializes a retract changelog into an upsert changelog before the sink.
 *
 * Flink: org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer
 * - keyed by the sink primary key (description JSON "grouping");
 * - state: per-key ordered list of rows (comboId list + VectorBatch side tables,
 *   state name "values"; the row order is the arrival order and is preserved);
 * - +I/+U appends the row (or replaces the stored row with the same upsert key when
 *   "inputUpsertKey" is present): the first row of a key emits +I, otherwise +U;
 * - -U/-D removes the first stored row equal on the compare columns (ignoring
 *   RowKind): removing the last row emits -D, removing the latest row additionally
 *   re-emits the new latest row as +U, removing a middle row emits nothing;
 * - a retract that finds no equal row is logged and swallowed (Flink 1.16.3
 *   behavior for state cleared by TTL);
 * - output rows keep the input schema, only the RowKind changes.
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

    static std::string_view getStringValue(omniruntime::vec::BaseVector* vector, int rowId)
    {
        if (vector->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
            return reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                vector)
                ->GetValue(rowId);
        }
        return reinterpret_cast<
            omniruntime::vec::Vector<
                omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
            vector)
            ->GetValue(rowId);
    }

    omnistream::VectorBatch* getVectorBatch(ComboId comboId)
    {
        auto keyGroup = VectorBatchUtil::getKeyGroup(comboId);
        auto sequenceNumber = VectorBatchUtil::getSequenceNumber(comboId);
        auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
        auto it = vectorBatchCacheMap_.find(vectorBatchId);
        if (it != vectorBatchCacheMap_.end()) {
            return it->second;
        }
        auto* vb = state_->getVectorBatch(keyGroup, sequenceNumber);
        if (vb == nullptr) {
            GErrorLog("SinkUpsertMaterializer: cannot find vector batch for comboId " + std::to_string(comboId));
            return nullptr;
        }
        vectorBatchCacheMap_.emplace(vectorBatchId, vb);
        return vb;
    }

    // Flink equalsIgnoreRowKind: compare the given columns (upsert-key projection
    // when present, otherwise the whole row), two nulls compare equal.
    bool rowsEqual(omnistream::VectorBatch* vb1, int rowId1, omnistream::VectorBatch* vb2, int rowId2)
    {
        for (auto col : compareIndices_) {
            bool null1 = vb1->Get(col)->IsNull(rowId1);
            bool null2 = vb2->Get(col)->IsNull(rowId2);
            if (null1 != null2) {
                return false;
            }
            if (null1) {
                continue;
            }
            switch (inputTypeIds_[col]) {
                case omniruntime::type::DataTypeId::OMNI_INT: {
                    auto val1 = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vb1->Get(col))->GetValue(rowId1);
                    auto val2 = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vb2->Get(col))->GetValue(rowId2);
                    if (val1 != val2) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    auto val1 = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb1->Get(col))->GetValue(rowId1);
                    auto val2 = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb2->Get(col))->GetValue(rowId2);
                    if (val1 != val2) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    auto val1 = reinterpret_cast<omniruntime::vec::Vector<double>*>(vb1->Get(col))->GetValue(rowId1);
                    auto val2 = reinterpret_cast<omniruntime::vec::Vector<double>*>(vb2->Get(col))->GetValue(rowId2);
                    if (val1 != val2) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    auto val1 = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vb1->Get(col))->GetValue(rowId1);
                    auto val2 = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vb2->Get(col))->GetValue(rowId2);
                    if (val1 != val2) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    if (getStringValue(vb1->Get(col), rowId1) != getStringValue(vb2->Get(col), rowId2)) {
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

    int findEqualRow(const std::vector<int64_t>& values, omnistream::VectorBatch* inputBatch, int inputRowId)
    {
        for (size_t idx = 0; idx < values.size(); idx++) {
            auto comboId = static_cast<ComboId>(values[idx]);
            auto* storedBatch = getVectorBatch(comboId);
            if (storedBatch == nullptr) {
                continue;
            }
            if (rowsEqual(storedBatch, VectorBatchUtil::getRowId(comboId), inputBatch, inputRowId)) {
                return static_cast<int>(idx);
            }
        }
        return -1;
    }

    void initOutputVector(omnistream::VectorBatch* out, int rowCount)
    {
        if (rowCount <= 0) {
            return;
        }
        for (auto typeId : inputTypeIds_) {
            switch (typeId) {
                case omniruntime::type::DataTypeId::OMNI_INT: {
                    out->Append(new omniruntime::vec::Vector<int32_t>(rowCount));
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    out->Append(new omniruntime::vec::Vector<int64_t>(rowCount));
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    out->Append(new omniruntime::vec::Vector<double>(rowCount));
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    out->Append(new omniruntime::vec::Vector<bool>(rowCount));
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    out->Append(new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                        rowCount));
                    break;
                }
                default: THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: unsupported output column type");
            }
        }
    }

    void copyRowToOutput(omnistream::VectorBatch* outputVB, ComboId comboId, int rowIndex)
    {
        int rowId = VectorBatchUtil::getRowId(comboId);
        auto* batch = getVectorBatch(comboId);
        outputVB->setTimestamp(rowIndex, batch->getTimestamp(rowId));
        for (size_t col = 0; col < inputTypeIds_.size(); col++) {
            auto* src = batch->Get(col);
            auto* dst = outputVB->Get(col);
            if (src->IsNull(rowId)) {
                if (inputTypeIds_[col] == omniruntime::type::DataTypeId::OMNI_CHAR ||
                    inputTypeIds_[col] == omniruntime::type::DataTypeId::OMNI_VARCHAR) {
                    // string vectors keep nulls in the container as well
                    reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(dst)
                        ->SetNull(rowIndex);
                } else {
                    dst->SetNull(rowIndex);
                }
                continue;
            }
            switch (inputTypeIds_[col]) {
                case omniruntime::type::DataTypeId::OMNI_INT: {
                    auto val = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(src)->GetValue(rowId);
                    reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(dst)->SetValue(rowIndex, val);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    auto val = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(src)->GetValue(rowId);
                    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(dst)->SetValue(rowIndex, val);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    auto val = reinterpret_cast<omniruntime::vec::Vector<double>*>(src)->GetValue(rowId);
                    reinterpret_cast<omniruntime::vec::Vector<double>*>(dst)->SetValue(rowIndex, val);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    auto val = reinterpret_cast<omniruntime::vec::Vector<bool>*>(src)->GetValue(rowId);
                    reinterpret_cast<omniruntime::vec::Vector<bool>*>(dst)->SetValue(rowIndex, val);
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        dst)
                        ->SetValue(rowIndex, getStringValue(src, rowId));
                    break;
                }
                default: THROW_LOGIC_EXCEPTION("SinkUpsertMaterializer: unsupported copy column type");
            }
        }
    }

    void processAccumulate(
        typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx,
        omnistream::VectorBatch* inputBatch,
        int rowId)
    {
        auto* values = state_->value();
        bool existed = values != nullptr;
        if (!existed) {
            values = new std::vector<int64_t>();
        }
        RowKind outKind = values->empty() ? RowKind::INSERT : RowKind::UPDATE_AFTER;
        auto newComboId = reuseComboIds_[rowId];
        if (hasUpsertKey_) {
            int index = findEqualRow(*values, inputBatch, rowId);
            if (index >= 0) {
                (*values)[index] = newComboId;
            } else {
                values->push_back(newComboId);
            }
        } else {
            values->push_back(newComboId);
        }
        if (!existed) {
            // Fresh key: hand the new vector over to the state. For an existing key
            // the in-place mutation of the stored vector is already visible (HEAP).
            state_->update(values);
        }
        collectedIds_.push_back(newComboId);
        collectedRowKinds_.push_back(outKind);
    }

    void processRetract(
        typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx,
        omnistream::VectorBatch* inputBatch,
        int rowId)
    {
        auto* values = state_->value();
        if (values == nullptr || values->empty()) {
            WARN_RELEASE("SinkUpsertMaterializer: retract matches no stored row (state cleared?); swallowed");
            return;
        }
        int index = findEqualRow(*values, inputBatch, rowId);
        if (index < 0) {
            WARN_RELEASE("SinkUpsertMaterializer: retract matches no stored row (state cleared?); swallowed");
            return;
        }
        auto removedComboId = (*values)[index];
        int lastIndexBeforeRemove = static_cast<int>(values->size()) - 1;
        values->erase(values->begin() + index);
        if (values->empty()) {
            collectedIds_.push_back(removedComboId);
            collectedRowKinds_.push_back(RowKind::DELETE);
        } else if (index == lastIndexBeforeRemove) {
            // Removed the latest row: re-emit the new latest row as +U (Flink retractRow).
            collectedIds_.push_back(values->back());
            collectedRowKinds_.push_back(RowKind::UPDATE_AFTER);
        }
        // Removing a middle row emits nothing. The (possibly empty) list stays in
        // state so the next accumulate of the same key emits +I again.
    }

    void cleanup()
    {
        reuseVectorBatchByKeyGroup_.clear();
        reuseOldRowIdsByKeyGroup_.clear();
        reuseKeys_.clear();
        reuseComboIds_.clear();
        collectedIds_.clear();
        collectedRowKinds_.clear();
        // HEAP side batches are owned by the state table; the cache only borrows them.
        vectorBatchCacheMap_.clear();
    }

    std::vector<std::string> inputTypes_;
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;
    std::vector<int32_t> groupingIndices_;
    std::vector<int32_t> inputUpsertKeyIndices_;
    bool hasUpsertKey_ = false;
    std::vector<int32_t> compareIndices_;
    std::unique_ptr<KeySelector<K>> keySelector_ = nullptr;

    ValueState<std::vector<int64_t>*>* state_ = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
    int32_t maxParallelism_ = 0;

    std::unordered_map<uint64_t, omnistream::VectorBatch*> vectorBatchCacheMap_{};
    std::unordered_map<int32_t, omnistream::VectorBatch*> reuseVectorBatchByKeyGroup_{};
    std::unordered_map<int32_t, std::vector<int32_t>> reuseOldRowIdsByKeyGroup_{};
    std::vector<K> reuseKeys_{};
    std::vector<ComboId> reuseComboIds_{};
    std::vector<int64_t> collectedIds_{};
    std::vector<RowKind> collectedRowKinds_{};
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
    maxParallelism_ = runtimeContext->getMaxNumberOfSubtasks();
    auto* stateDescriptor = new ValueStateDescriptor<std::vector<int64_t>*>("values", new SortedVectorLong());
    state_ = runtimeContext->template getState<std::vector<int64_t>*>(stateDescriptor);
}

template <typename K>
void SinkUpsertMaterializer<K>::processBatch(
    omnistream::VectorBatch* inputBatch,
    typename KeyedProcessFunction<K, RowData*, omnistream::VectorBatch*>::Context& ctx,
    TimestampedCollector& out)
{
    auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
    int rowCnt = inputBatchGuard->GetRowCount();
    collectedIds_.clear();
    collectedRowKinds_.clear();

    // Register the input rows in the side tables and assign a comboId per row,
    // so emissions can reference both input rows and previously stored rows.
    for (int i = 0; i < rowCnt; i++) {
        auto key = keySelector_->getKey(inputBatchGuard.get(), i);
        reuseKeys_.push_back(key);
        auto keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, maxParallelism_);
        auto& oldRowIds = reuseOldRowIdsByKeyGroup_[keyGroup];
        auto newRowId = static_cast<int32_t>(oldRowIds.size());
        auto comboId = VectorBatchUtil::getComboId(keyGroup, state_->getNextSequenceNumber(keyGroup), newRowId);
        reuseComboIds_.push_back(comboId);
        oldRowIds.push_back(i);
    }
    bool shouldSplitVectorBatch = reuseOldRowIdsByKeyGroup_.size() > 1 ||
        (reuseOldRowIdsByKeyGroup_.size() == 1 && reuseOldRowIdsByKeyGroup_.begin()->second.size() !=
                                                      static_cast<size_t>(rowCnt));
    for (auto& [keyGroup, oldRowIds] : reuseOldRowIdsByKeyGroup_) {
        omnistream::VectorBatch* splitBatch = nullptr;
        if (shouldSplitVectorBatch) {
            splitBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(inputBatchGuard.get(), oldRowIds);
        } else {
            splitBatch = inputBatchGuard.get();
        }
        auto sequenceNumber = state_->getNextSequenceNumber(keyGroup);
        auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
        reuseVectorBatchByKeyGroup_[keyGroup] = splitBatch;
        vectorBatchCacheMap_.emplace(vectorBatchId, splitBatch);
    }
    state_->addVectorBatches(reuseVectorBatchByKeyGroup_);

    // Row-by-row materialization: keyed state is addressed by key value, so rows
    // sharing a key chain through the same state slot in arrival order.
    for (int i = 0; i < rowCnt; i++) {
        ctx.setCurrentKey(reuseKeys_[i]);
        if (isAccumulateMsg(inputBatchGuard->getRowKind(i))) {
            processAccumulate(ctx, inputBatchGuard.get(), i);
        } else {
            processRetract(ctx, inputBatchGuard.get(), i);
        }
    }

    auto* outputVB = new omnistream::VectorBatch(static_cast<int32_t>(collectedIds_.size()));
    initOutputVector(outputVB, static_cast<int32_t>(collectedIds_.size()));
    for (size_t i = 0; i < collectedIds_.size(); i++) {
        copyRowToOutput(outputVB, static_cast<ComboId>(collectedIds_[i]), static_cast<int32_t>(i));
    }
    if (!collectedRowKinds_.empty()) {
        outputVB->setRowKinds(0, collectedRowKinds_.data(), collectedRowKinds_.size());
    }
    out.collect(outputVB);

    if (!shouldSplitVectorBatch) {
        // The input batch itself was registered as a side-table batch and is now
        // owned by the state.
        inputBatchGuard.release();
    }
    cleanup();
}

#endif // OMNISTREAM_SINKUPSERTMATERIALIZER_H
