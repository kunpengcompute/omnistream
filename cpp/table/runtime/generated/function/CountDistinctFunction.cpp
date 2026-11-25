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

#include <optional>
#include <algorithm>
#include <unordered_set>
#include "CountDistinctFunction.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"

namespace {
TypeSerializer* createOwnedDistinctSerializer(DataTypeId typeId)
{
    switch (typeId) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_LONG: return new LongSerializer();
        default: throw std::runtime_error("Data type is not supported.");
    }
}
} // namespace

bool CountDistinctFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            isEqual = *r1->getInt(valueIndex) == *r2->getInt(valueIndex);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            isEqual = *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}

void CountDistinctFunction::open(StateDataViewStore* store)
{
    this->store = store;
    pendingDistinctUpdates.reserve(64);
    auto* perKeyViewStore = reinterpret_cast<PerKeyStateDataViewStore<RowData*>*>(store);
    // todo support more data types
    switch (typeId) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_LONG: {
            distinctMapView = reinterpret_cast<KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long>*>(
                perKeyViewStore->getStateMapView<VoidNamespace, long, long>(
                    "distinct_acc" + std::to_string(aggFuncIndex),
                    true,
                    createOwnedDistinctSerializer(typeId),
                    createOwnedDistinctSerializer(typeId)));

            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
}

void CountDistinctFunction::accumulate(RowData* accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue = 0L;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            fieldValue = isFieldNull ? -1L : *accInput->getInt(aggIdx);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            fieldValue = isFieldNull ? -1L : *accInput->getLong(aggIdx);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
    bool shouldDoAccumulate = !hasFilter || (hasFilter && *accInput->getBool(filterIndex));
    if (shouldDoAccumulate && !isFieldNull) {
        const bool existsInState = distinctMapView->contains(std::optional<long>{fieldValue});
        if (!existsInState) {
            if (!valueIsNull) {
                aggCount++;
            } else {
                aggCount = 1L;
                valueIsNull = false;
            }
            distinctMapView->put(std::optional<long>{fieldValue}, 0L);
        }
    }

    LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull);
}

void CountDistinctFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    if (backend == 2) {
        this->accumulateInRocksDB(input, indices);
    } else {
        auto columnData = input->Get(aggIdx);
        const bool hasFilterCol = hasFilter;
        const auto filterData =
            hasFilterCol ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;
        for (int rowIndex : indices) {
            bool shouldDoAccumulate = true;
            if (hasFilterCol) {
                bool isFilterNull = filterData->IsNull(rowIndex);
                shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
            }
            if (!shouldDoAccumulate) continue;
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (isFieldNull) {
                continue;
            }

            long fieldValue = 0L;
            switch (typeId) {
                case DataTypeId::OMNI_INT: {
                    fieldValue = dynamic_cast<omniruntime::vec::Vector<int>*>(columnData)->GetValue(rowIndex);
                    break;
                }
                case DataTypeId::OMNI_LONG: {
                    fieldValue = dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
                    break;
                }
                default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
            }
            const bool existsInState = distinctMapView->contains(std::optional<long>{fieldValue});
            if (!existsInState) {
                if (!valueIsNull) {
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
                distinctMapView->put(std::optional<long>{fieldValue}, 0L);
            }
        }
        LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
    }
}

void CountDistinctFunction::accumulateInRocksDB(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);
    const bool hasFilterCol = hasFilter;
    const auto filterData =
        hasFilterCol ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;
    auto* intColumn =
        (typeId == DataTypeId::OMNI_INT) ? dynamic_cast<omniruntime::vec::Vector<int>*>(columnData) : nullptr;
    auto* longColumn =
        (typeId == DataTypeId::OMNI_LONG) ? dynamic_cast<omniruntime::vec::Vector<long>*>(columnData) : nullptr;

    if ((typeId == DataTypeId::OMNI_INT && intColumn == nullptr) ||
        (typeId == DataTypeId::OMNI_LONG && longColumn == nullptr)) {
        LOG("Input column type mismatch for COUNT DISTINCT.");
        throw std::runtime_error("Input column type mismatch for COUNT DISTINCT.");
    }

    // Stage 1: deduplicate incoming records within this batch.
    std::unordered_set<long> batchDistinctKeys;
    batchDistinctKeys.reserve(indices.size());

    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }
        if (!shouldDoAccumulate) {
            continue;
        }

        if (columnData->IsNull(rowIndex)) {
            continue;
        }

        long fieldValue = 0L;
        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue = static_cast<long>(intColumn->GetValue(rowIndex));
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue = longColumn->GetValue(rowIndex);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }

        batchDistinctKeys.emplace(fieldValue);
    }

    // Stage 2: compare deduplicated keys with state and collect new keys for deferred batch flush.
    for (const long fieldValue : batchDistinctKeys) {
        const bool existsInState = distinctMapView->contains(std::optional<long>{fieldValue});
        if (existsInState) {
            continue;
        }

        if (currentGroupKey != nullptr) {
            pendingDistinctUpdates.emplace_back(currentGroupKey, fieldValue, 0L);
        }

        if (!valueIsNull) {
            aggCount++;
        } else {
            aggCount = 1L;
            valueIsNull = false;
        }
    }

    LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
}

void CountDistinctFunction::retract(RowData* retractInput)
{
}

void CountDistinctFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
}

void CountDistinctFunction::merge(RowData* otherAcc)
{
}

void CountDistinctFunction::setAccumulators(RowData* acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex);
}

void CountDistinctFunction::resetAccumulators()
{
    aggCount = (static_cast<long>(0L));
    valueIsNull = false;

    if (currentGroupKey != nullptr && !pendingDistinctUpdates.empty()) {
        auto pendingIt = std::remove_if(
            pendingDistinctUpdates.begin(),
            pendingDistinctUpdates.end(),
            [&](const std::tuple<RowData*, long, long>& update) {
                if (!std::equal_to<RowData*>{}(std::get<0>(update), currentGroupKey)) {
                    return false;
                }
                return true;
            });
        pendingDistinctUpdates.erase(pendingIt, pendingDistinctUpdates.end());
    }

    auto* entries = distinctMapView->entries();
    if (entries != nullptr) {
        std::vector<long> keysToRemove;
        keysToRemove.reserve(entries->size());
        for (const auto& entry : *entries) {
            keysToRemove.push_back(entry.first);
        }
        for (const long distinctKey : keysToRemove) {
            distinctMapView->remove(std::optional<long>{distinctKey});
        }
    }

    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull);
}

void CountDistinctFunction::getAccumulators(BinaryRowData* accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount);
}

void CountDistinctFunction::createAccumulators(BinaryRowData* accumulators)
{
    if (false) { // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create acc");
}

void CountDistinctFunction::getValue(BinaryRowData* newAggValue)
{
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        newAggValue->setLong(valueIndex, aggCount);
    }
    LOG("Get value: " << aggCount);
}

void CountDistinctFunction::setCurrentGroupKey(RowData* key)
{
    this->currentGroupKey = key;
}

void CountDistinctFunction::updateInnerState()
{
    if (!pendingDistinctUpdates.empty()) {
        distinctMapView->putByBatch(pendingDistinctUpdates);
    }
    pendingDistinctUpdates.clear();
    this->distinctMapView->cleanup();
}

void CountDistinctFunction::cleanup()
{
    pendingDistinctUpdates.clear();
}

void CountDistinctFunction::close()
{
    pendingDistinctUpdates.clear();
}

CountDistinctFunction::~CountDistinctFunction()
{
}
