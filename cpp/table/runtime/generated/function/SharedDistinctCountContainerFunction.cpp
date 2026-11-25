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

#include "SharedDistinctCountContainerFunction.h"
#include <algorithm>
#include <optional>
#include <stdexcept>
#include "core/typeutils/ExternalSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"

using namespace omniruntime::type;

namespace {
TypeSerializer* createOwnedSharedDistinctSerializer(DataTypeId typeId)
{
    switch (typeId) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_LONG: return new LongSerializer();
        default: throw std::runtime_error("Shared DISTINCT only supports INT/LONG key types.");
    }
}
} // namespace

SharedDistinctCountContainerFunction::SharedDistinctCountContainerFunction(std::string stateName)
    : stateName_(std::move(stateName))
{
}

void SharedDistinctCountContainerFunction::addDistinctEntry(
    int aggFuncIndex, const std::string& aggType, int aggIdx, int filterIndex, const std::string& inputType)
{
    auto* entry = new DistinctEntry();
    entry->aggFuncIndex = aggFuncIndex;
    entry->aggType = aggType;
    entry->aggIdx = aggIdx;
    entry->filterIndex = filterIndex;
    entry->inputType = inputType;
    entry->typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
    entries_.push_back(entry);
    finalized_ = false;
}

void SharedDistinctCountContainerFunction::getOrCreateGroup(DistinctEntry* entry)
{
    int aggIdx = entry->aggIdx;
    auto found = distinctGroupMap.find(aggIdx);
    if (found != distinctGroupMap.end()) {
        groups_[found->second].groupEntries.push_back(entry);
    } else {
        DistinctGroup group;
        group.aggIdx = aggIdx;
        group.typeId = entry->typeId;
        group.stateName = stateName_ + "_" + std::to_string(groups_.size());
        // group.groupEntries.push_back(entry);
        groups_.push_back(std::move(group));
        groups_.back().groupEntries.push_back(entry);
        distinctGroupMap.emplace(aggIdx, groups_.size() - 1);
    }
}

void SharedDistinctCountContainerFunction::finalizeEntries()
{
    if (finalized_) {
        return;
    }
    if (entries_.empty()) {
        throw std::runtime_error("SharedDistinctCountContainerFunction has no entries.");
    }

    entryIndexToAggFuncIndex.clear();
    entryIndexToAggFuncIndex.reserve(entries_.size());
    groups_.clear();
    distinctGroupMap.clear();

    for (int i = 0; i < entries_.size(); ++i) {
        auto& entry = entries_[i];
        if (entryIndexToAggFuncIndex.find(entry->aggFuncIndex) != entryIndexToAggFuncIndex.end()) {
            throw std::runtime_error("Duplicated aggFuncIndex in shared DISTINCT container.");
        }
        if (entry->typeId != DataTypeId::OMNI_INT && entry->typeId != DataTypeId::OMNI_LONG) {
            throw std::runtime_error("Shared DISTINCT only supports INT/LONG key types.");
        }
        getOrCreateGroup(entry);
        entryIndexToAggFuncIndex.emplace(entry->aggFuncIndex, i);
    }

    for (auto& group : groups_) {
        if (group.groupEntries.size() > 64) {
            throw std::runtime_error(
                "SharedDistinctCountContainerFunction supports at most 64 DISTINCT entries per key column.");
        }

        group.pendingDistinctUpdates.reserve(500);
        for (int bitOffset = 0; bitOffset < group.groupEntries.size(); ++bitOffset) {
            group.groupEntries[bitOffset]->bitOffset = static_cast<std::uint8_t>(bitOffset);
        }
    }

    finalized_ = true;
}

void SharedDistinctCountContainerFunction::bindEntryAccValueIndex(int aggFuncIndex, int accIndex, int valueIndex)
{
    if (!finalized_) {
        throw std::runtime_error("bindEntryIndices called before finalizeEntries.");
    }
    auto it = entryIndexToAggFuncIndex.find(aggFuncIndex);
    if (it == entryIndexToAggFuncIndex.end()) {
        throw std::runtime_error("Unknown aggFuncIndex for shared DISTINCT entry binding.");
    }
    auto& entry = entries_[it->second];
    entry->accIndex = accIndex;
    entry->valueIndex = valueIndex;
}

void SharedDistinctCountContainerFunction::bindAccValueIndex(int accStartIndex, int valueStartIndex)
{
    finalizeEntries();
    for (auto& entry : entries_) {
        entry->accIndex = accStartIndex++;
        entry->valueIndex = valueStartIndex++;
    }
}

bool SharedDistinctCountContainerFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    for (const auto& entry : entries_) {
        if (entry->valueIndex < 0) {
            continue;
        }
        if (r1->isNullAt(entry->valueIndex) || r2->isNullAt(entry->valueIndex)) {
            return false;
        }
        if (*r1->getLong(entry->valueIndex) != *r2->getLong(entry->valueIndex)) {
            return false;
        }
    }
    return true;
}

void SharedDistinctCountContainerFunction::open(StateDataViewStore* store)
{
    finalizeEntries();
    store_ = store;
    auto* perKeyViewStore = reinterpret_cast<PerKeyStateDataViewStore<RowData*>*>(store_);

    for (auto& group : groups_) {
        group.distinctMapView = reinterpret_cast<KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long>*>(
            perKeyViewStore->getStateMapView<VoidNamespace, long, long>(
                group.stateName,
                true,
                ExternalSerializer::of(BasicLogicalType::BIGINT),
                ExternalSerializer::of(BasicLogicalType::BIGINT)));
    }
}

bool SharedDistinctCountContainerFunction::shouldAccumulateForEntry(DistinctEntry* entry, RowData* inputRow) const
{
    if (entry->filterIndex < 0) {
        return true;
    }
    const bool isFilterNull = inputRow->isNullAt(entry->filterIndex);
    return !isFilterNull && *inputRow->getBool(entry->filterIndex);
}

std::uint64_t SharedDistinctCountContainerFunction::collectCandidateMask(
    RowData* inputRow, const DistinctGroup& group) const
{
    std::uint64_t candidateMask = 0ULL;
    for (auto entry : group.groupEntries) {
        if (shouldAccumulateForEntry(entry, inputRow)) {
            candidateMask |= (1ULL << entry->bitOffset);
        }
    }
    return candidateMask;
}

void SharedDistinctCountContainerFunction::applyMaskDelta(std::size_t groupIndex, std::uint64_t deltaMask)
{
    auto& group = groups_[groupIndex];
    for (auto& entry : group.groupEntries) {
        if ((deltaMask & (1ULL << entry->bitOffset)) == 0ULL) {
            continue;
        }
        if (entry->valueIsNull) {
            entry->aggCount = 1L;
            entry->valueIsNull = false;
        } else {
            entry->aggCount++;
        }
    }
}

long SharedDistinctCountContainerFunction::getRowFieldValue(
    RowData* row, int aggIdx, DataTypeId typeId, bool& isNull) const
{
    isNull = row->isNullAt(aggIdx);
    if (isNull) {
        return 0L;
    }

    switch (typeId) {
        case DataTypeId::OMNI_INT: return static_cast<long>(*row->getInt(aggIdx));
        case DataTypeId::OMNI_LONG: return *row->getLong(aggIdx);
        default: throw std::runtime_error("Unsupported shared DISTINCT key type.");
    }
}

void SharedDistinctCountContainerFunction::accumulate(RowData* accInput)
{
    for (std::size_t groupIndex = 0; groupIndex < groups_.size(); ++groupIndex) {
        auto& group = groups_[groupIndex];
        if (group.distinctMapView == nullptr) {
            continue;
        }

        const std::uint64_t candidateMask = collectCandidateMask(accInput, group);
        if (candidateMask == 0ULL) {
            continue;
        }

        bool isNull = false;
        const long fieldValue = getRowFieldValue(accInput, group.aggIdx, group.typeId, isNull);
        if (isNull) {
            continue;
        }

        const auto existingValue = group.distinctMapView->get(std::optional<long>{fieldValue});
        const std::uint64_t existingMask =
            existingValue.has_value() ? static_cast<std::uint64_t>(*existingValue) : 0ULL;
        const std::uint64_t newMask = existingMask | candidateMask;
        const std::uint64_t deltaMask = newMask ^ existingMask;
        if (deltaMask == 0ULL) {
            continue;
        }

        applyMaskDelta(groupIndex, deltaMask);

        group.distinctMapView->put(std::optional<long>{fieldValue}, static_cast<long>(newMask));
    }
}

void SharedDistinctCountContainerFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    if (indices.empty()) {
        return;
    }

    for (std::size_t groupIndex = 0; groupIndex < groups_.size(); ++groupIndex) {
        auto& group = groups_[groupIndex];
        if (group.distinctMapView == nullptr) {
            continue;
        }

        auto* columnData = input->Get(group.aggIdx);
        auto* intColumn =
            (group.typeId == DataTypeId::OMNI_INT) ? dynamic_cast<omniruntime::vec::Vector<int>*>(columnData) : nullptr;
        auto* longColumn = (group.typeId == DataTypeId::OMNI_LONG)
                               ? dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)
                               : nullptr;

        if ((group.typeId == DataTypeId::OMNI_INT && intColumn == nullptr) ||
            (group.typeId == DataTypeId::OMNI_LONG && longColumn == nullptr)) {
            throw std::runtime_error("Input column type mismatch for shared DISTINCT.");
        }

        std::vector<omniruntime::vec::Vector<bool>*> filterColumns;
        filterColumns.reserve(group.groupEntries.size());
        for (const auto& entry : group.groupEntries) {
            if (entry->filterIndex >= 0) {
                filterColumns.push_back(
                    reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(entry->filterIndex)));
            } else {
                filterColumns.push_back(nullptr);
            }
        }

        std::unordered_map<long, std::uint64_t> batchRequestMasks;
        batchRequestMasks.reserve(indices.size());

        for (int rowIndex : indices) {
            if (columnData->IsNull(rowIndex)) {
                continue;
            }

            std::uint64_t candidateMask = 0ULL;
            for (std::size_t i = 0; i < group.groupEntries.size(); ++i) {
                const auto& entry = group.groupEntries[i];
                bool shouldAccumulate = true;
                if (entry->filterIndex >= 0) {
                    auto* filterData = filterColumns[i];
                    const bool isFilterNull = filterData->IsNull(rowIndex);
                    shouldAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
                }
                if (shouldAccumulate) {
                    candidateMask |= (1ULL << entry->bitOffset);
                }
            }
            if (candidateMask == 0ULL) {
                continue;
            }

            long fieldValue = 0L;
            if (group.typeId == DataTypeId::OMNI_INT) {
                fieldValue = static_cast<long>(intColumn->GetValue(rowIndex));
            } else {
                fieldValue = longColumn->GetValue(rowIndex);
            }
            batchRequestMasks[fieldValue] |= candidateMask;
        }

        for (const auto& pair : batchRequestMasks) {
            const long fieldValue = pair.first;
            const std::uint64_t candidateMask = pair.second;

            const auto existingValue = group.distinctMapView->get(std::optional<long>{fieldValue});
            const std::uint64_t existingMask =
                existingValue.has_value() ? static_cast<std::uint64_t>(*existingValue) : 0ULL;
            const std::uint64_t newMask = existingMask | candidateMask;
            const std::uint64_t deltaMask = newMask & (~existingMask);
            if (deltaMask == 0ULL) {
                continue;
            }

            applyMaskDelta(groupIndex, deltaMask);
            if (backend == 2 && currentGroupKey_ != nullptr) {
                group.pendingDistinctUpdates[currentGroupKey_].emplace_back(fieldValue, static_cast<long>(newMask));
            } else {
                group.distinctMapView->put(std::optional<long>{fieldValue}, static_cast<long>(newMask));
            }
        }
    }
}

void SharedDistinctCountContainerFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("SharedDistinctCountContainerFunction does not support merge.");
}

void SharedDistinctCountContainerFunction::setAccumulators(RowData* acc)
{
    for (auto& entry : entries_) {
        if (entry->accIndex < 0) {
            continue;
        }
        entry->valueIsNull = acc->isNullAt(entry->accIndex);
        entry->aggCount = entry->valueIsNull ? -1L : *acc->getLong(entry->accIndex);
    }
}

void SharedDistinctCountContainerFunction::resetAccumulators()
{
    for (auto& entry : entries_) {
        entry->aggCount = 0L;
        entry->valueIsNull = false;
    }

    for (auto& group : groups_) {
        group.pendingDistinctUpdates.clear();
        if (group.distinctMapView == nullptr) {
            continue;
        }
        auto* entries = group.distinctMapView->entries();
        if (entries == nullptr) {
            continue;
        }
        std::vector<long> keysToRemove;
        keysToRemove.reserve(entries->size());
        for (const auto& entry : *entries) {
            keysToRemove.push_back(entry.first);
        }
        for (long distinctKey : keysToRemove) {
            group.distinctMapView->remove(std::optional<long>{distinctKey});
        }
    }
}

void SharedDistinctCountContainerFunction::getAccumulators(BinaryRowData* accumulators)
{
    for (const auto& entry : entries_) {
        if (entry->accIndex < 0) {
            continue;
        }
        if (entry->valueIsNull) {
            accumulators->setNullAt(entry->accIndex);
        } else {
            accumulators->setLong(entry->accIndex, entry->aggCount);
        }
    }
}

void SharedDistinctCountContainerFunction::createAccumulators(BinaryRowData* accumulators)
{
    for (const auto& entry : entries_) {
        if (entry->accIndex >= 0) {
            accumulators->setLong(entry->accIndex, 0L);
        }
    }
}

void SharedDistinctCountContainerFunction::getValue(BinaryRowData* aggValue)
{
    for (const auto& entry : entries_) {
        if (entry->valueIndex < 0) {
            continue;
        }
        if (entry->valueIsNull) {
            aggValue->setNullAt(entry->valueIndex);
        } else {
            aggValue->setLong(entry->valueIndex, entry->aggCount);
        }
    }
}

void SharedDistinctCountContainerFunction::cleanup()
{
    for (auto& group : groups_) {
        group.pendingDistinctUpdates.clear();
    }
}

void SharedDistinctCountContainerFunction::close()
{
    for (auto& group : groups_) {
        group.pendingDistinctUpdates.clear();
    }
}

void SharedDistinctCountContainerFunction::setCurrentGroupKey(RowData* key)
{
    currentGroupKey_ = key;
}

void SharedDistinctCountContainerFunction::updateInnerState()
{
    for (auto& group : groups_) {
        if (group.distinctMapView != nullptr && !group.pendingDistinctUpdates.empty()) {
            group.distinctMapView->putByBatch(group.pendingDistinctUpdates);
        }
        group.pendingDistinctUpdates.clear();
        if (group.distinctMapView != nullptr) {
            group.distinctMapView->cleanup();
        }
    }
}

long SharedDistinctCountContainerFunction::getRowFieldValueFromVB(
    omnistream::VectorBatch* input, int columnIdx, int rowIdx, DataTypeId typeId, bool& isNull) const
{
    auto* columnData = input->Get(columnIdx);
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            auto* intColumn = dynamic_cast<omniruntime::vec::Vector<int>*>(columnData);
            if (intColumn == nullptr) {
                throw std::runtime_error("Input column type mismatch for shared DISTINCT.");
            }
            isNull = intColumn->IsNull(rowIdx);
            return isNull ? 0L : static_cast<long>(intColumn->GetValue(rowIdx));
        }
        case DataTypeId::OMNI_LONG: {
            auto* longColumn = dynamic_cast<omniruntime::vec::Vector<long>*>(columnData);
            if (longColumn == nullptr) {
                throw std::runtime_error("Input column type mismatch for shared DISTINCT.");
            }
            isNull = longColumn->IsNull(rowIdx);
            return isNull ? 0L : longColumn->GetValue(rowIdx);
        }
        default: throw std::runtime_error("Unsupported shared DISTINCT key type.");
    }
}
