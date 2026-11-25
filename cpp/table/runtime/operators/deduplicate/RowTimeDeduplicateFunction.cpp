/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include "RowTimeDeduplicateFunction.h"

#include <memory>
#include <string>
#include <string_view>

#include "core/api/common/state/ValueStateDescriptor.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/LogicalType.h"

RowTimeDeduplicateFunction::RowTimeDeduplicateFunction(const nlohmann::json& config)
{
    generateUpdateBefore_ = config["generateUpdateBefore"];
    generateInsert_ = config["generateInsert"];
    rowtimeIndex_ = config["rowtimeIndex"];
    keepLastRow_ = config["keepLastRow"];
    keyIndices_ = config["grouping"].get<std::vector<int32_t>>();
    inputTypes_ = config["inputTypes"].get<std::vector<std::string>>();
    keyedTypes_ = getKeyedTypes(keyIndices_, inputTypes_);
    inputTypeIds_.reserve(inputTypes_.size());
    for (const auto& inputType : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(inputType));
    }
    groupByKeySelector_ = std::make_unique<KeySelector<RowData*>>(keyedTypes_, keyIndices_);
}

void RowTimeDeduplicateFunction::open(const Configuration& config)
{
    (void)config;
    LOG("RowTimeDeduplicateFunction open");
    auto* rowTypeInfo = InternalTypeInfo::ofRowType(new omnistream::RowType(true, inputTypes_));
    auto* stateDescriptor = new ValueStateDescriptor<RowData*>("deduplicate-state", rowTypeInfo);
    stateDescriptor->SetStateSerializer(rowTypeInfo->getTypeSerializer());

    auto* runtimeContext = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext());
    recordState_ = runtimeContext->getState<RowData*>(stateDescriptor);
    backendType_ = runtimeContext->getStateType();
    omnistream::checkStateType(backendType_, "RowTimeDeduplicateFunction");
    if (backendType_ == omnistream::StateType::ROCKSDB) {
        auto* rocksdbState = dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(recordState_);
        if (rocksdbState == nullptr) {
            THROW_LOGIC_EXCEPTION("RowTimeDeduplicateFunction failed to resolve RocksDB ValueState");
        }
        rocksdbCacheEnabled_ = rocksdbState->stateCache->getSizeLimit() != 0;
    } else if (backendType_ != omnistream::StateType::HEAP) {
        THROW_LOGIC_EXCEPTION("RowTimeDeduplicateFunction only supports heap and RocksDB state backends");
    }
    LOG("RowTimeDeduplicateFunction open finish");
}

void RowTimeDeduplicateFunction::processBatch(omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out)
{
    auto inputGuard = std::unique_ptr<omnistream::VectorBatch>(inputVB);
    auto candidates = selectBatchCandidates(inputVB);
    std::vector<PendingUpdate> updates;
    updates.reserve(candidates.rowIds.size());
    std::vector<RowData*> outputRows;
    std::vector<RowKind> outputKinds;
    std::vector<int64_t> outputTimestamps;

    for (const auto& [key, currentRowId] : candidates.rowIds) {
        ctx.setCurrentKey(key);
        bool cacheHit = false;
        if (backendType_ == omnistream::StateType::ROCKSDB && rocksdbCacheEnabled_) {
            auto* rocksdbState = static_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(recordState_);
            cacheHit = rocksdbState->stateCache->contains(key, rocksdbState->getNamespace());
        }
        RowData* previousRow = recordState_->value();
        std::unique_ptr<RowData> ownedPreviousRow;
        if (backendType_ == omnistream::StateType::ROCKSDB && previousRow != nullptr) {
            // Misses return an owned row. Copy hits before later lookups can clear the cache.
            ownedPreviousRow.reset(cacheHit ? previousRow->copy() : previousRow);
            previousRow = ownedPreviousRow.get();
        }
        const bool isInsert = previousRow == nullptr;
        if (!isInsert && !shouldReplace(previousRow, inputVB, currentRowId)) {
            continue;
        }

        std::unique_ptr<RowData> currentRow(inputVB->extractRowData(currentRowId));
        if (currentRow == nullptr) {
            THROW_RUNTIME_ERROR("RowTimeDeduplicateFunction failed to extract selected RowData");
        }

        const int64_t timestamp = inputVB->getTimestamp(currentRowId);
        if (isInsert) {
            outputRows.push_back(currentRow.get());
            outputKinds.push_back(generateUpdateBefore_ || generateInsert_ ? RowKind::INSERT : RowKind::UPDATE_AFTER);
            outputTimestamps.push_back(timestamp);
        } else {
            if (generateUpdateBefore_) {
                outputRows.push_back(previousRow);
                outputKinds.push_back(RowKind::UPDATE_BEFORE);
                outputTimestamps.push_back(timestamp);
            }
            outputRows.push_back(currentRow.get());
            outputKinds.push_back(RowKind::UPDATE_AFTER);
            outputTimestamps.push_back(timestamp);
        }
        PendingUpdate update;
        update.key = key;
        update.previousRow = previousRow;
        update.ownedPreviousRow = std::move(ownedPreviousRow);
        update.currentRow = std::move(currentRow);
        update.isInsert = isInsert;
        updates.push_back(std::move(update));
    }

    std::unique_ptr<omnistream::VectorBatch> outputBatch(buildOutputBatch(outputRows, outputKinds, outputTimestamps));
    updateState(updates, ctx);
    out.collect(outputBatch.release());
}

RowTimeDeduplicateFunction::BatchCandidates RowTimeDeduplicateFunction::selectBatchCandidates(
    omnistream::VectorBatch* inputVB)
{
    BatchCandidates candidates;
    candidates.rowIds.reserve(inputVB->GetRowCount());
    candidates.ownedKeys.reserve(inputVB->GetRowCount());
    const auto* rowtimeVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(inputVB->Get(rowtimeIndex_));
    for (int32_t rowId = 0; rowId < inputVB->GetRowCount(); ++rowId) {
        std::unique_ptr<RowData> key(groupByKeySelector_->getKey(inputVB, rowId));
        auto existing = candidates.rowIds.find(key.get());
        if (existing == candidates.rowIds.end()) {
            candidates.rowIds.emplace(key.get(), rowId);
            candidates.ownedKeys.push_back(std::move(key));
            continue;
        }

        const int32_t previousRowId = existing->second;
        const int64_t previousTime = rowtimeVector->GetValue(previousRowId);
        const int64_t currentTime = rowtimeVector->GetValue(rowId);
        const bool replace = keepLastRow_ ? previousTime <= currentTime : currentTime < previousTime;
        if (replace) {
            existing->second = rowId;
        }
    }
    return candidates;
}

bool RowTimeDeduplicateFunction::shouldReplace(
    RowData* previousRow, omnistream::VectorBatch* currentVB, int32_t currentRowId) const
{
    if (previousRow->isNullAt(rowtimeIndex_) || currentVB->Get(rowtimeIndex_)->IsNull(currentRowId)) {
        return false;
    }
    const int64_t previousTime = *previousRow->getLong(rowtimeIndex_);
    const int64_t currentTime =
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(currentVB->Get(rowtimeIndex_))->GetValue(currentRowId);
    return keepLastRow_ ? previousTime <= currentTime : currentTime < previousTime;
}

omnistream::VectorBatch* RowTimeDeduplicateFunction::buildOutputBatch(
    const std::vector<RowData*>& rows,
    const std::vector<RowKind>& rowKinds,
    const std::vector<int64_t>& timestamps) const
{
    const int32_t rowCount = static_cast<int32_t>(rows.size());
    auto* output = new omnistream::VectorBatch(rowCount);
    for (int32_t column = 0; column < static_cast<int32_t>(inputTypeIds_.size()); ++column) {
        switch (inputTypeIds_[column]) {
            case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case OMNI_TIMESTAMP:
            case OMNI_LONG: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(rowCount);
                for (int32_t row = 0; row < rowCount; ++row) {
                    if (rows[row]->isNullAt(column)) {
                        vector->SetNull(row);
                    } else {
                        vector->SetValue(row, *rows[row]->getLong(column));
                    }
                }
                output->Append(vector);
                break;
            }
            case OMNI_INT: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(rowCount);
                for (int32_t row = 0; row < rowCount; ++row) {
                    if (rows[row]->isNullAt(column)) {
                        vector->SetNull(row);
                    } else {
                        vector->SetValue(row, *rows[row]->getInt(column));
                    }
                }
                output->Append(vector);
                break;
            }
            case OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(rowCount);
                for (int32_t row = 0; row < rowCount; ++row) {
                    if (rows[row]->isNullAt(column)) {
                        vector->SetNull(row);
                    } else {
                        vector->SetValue(row, *rows[row]->getBool(column));
                    }
                }
                output->Append(vector);
                break;
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                using StringVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
                auto* vector = new StringVector(rowCount);
                for (int32_t row = 0; row < rowCount; ++row) {
                    if (rows[row]->isNullAt(column)) {
                        vector->SetNull(row);
                    } else {
                        auto value = std::string(rows[row]->getStringView(column));
                        omniruntime::vec::VectorHelper::VectorSetValue<OMNI_VARCHAR>(vector, row, &value);
                    }
                }
                output->Append(vector);
                break;
            }
            default: THROW_RUNTIME_ERROR("RowTimeDeduplicateFunction unsupported output column type");
        }
    }

    for (int32_t row = 0; row < rowCount; ++row) {
        output->setRowKind(row, rowKinds[row]);
        output->setTimestamp(row, timestamps[row]);
    }
    return output;
}

void RowTimeDeduplicateFunction::updateState(std::vector<PendingUpdate>& updates, Context& ctx)
{
    for (auto& update : updates) {
        ctx.setCurrentKey(update.key);
        // The batch remains vectorized until a key actually wins. Only that row is
        // materialized and handed to the ordinary Flink-compatible ValueState.
        // Heap state owns pointer values, so remove the superseded row before
        // replacing it. RocksDB either owns rows through its cache or serializes
        // them synchronously; release temporary rows only in the latter mode.
        if (!update.isInsert && backendType_ == omnistream::StateType::HEAP) {
            recordState_->clear();
        }
        recordState_->update(update.currentRow.get(), false);
        if (backendType_ == omnistream::StateType::HEAP) {
            // Heap state now owns the selected row.
            update.currentRow.release();
        }
        // RocksDB serializes the row immediately or deep-copies it into the
        // cache. The unique_ptrs release local current/uncached-previous rows.
    }
}

std::vector<int32_t> RowTimeDeduplicateFunction::getKeyedTypes(
    const std::vector<int32_t>& keyedIndex, const std::vector<std::string>& inputTypes)
{
    std::vector<int32_t> keyedTypes;
    keyedTypes.reserve(keyedIndex.size());
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}
