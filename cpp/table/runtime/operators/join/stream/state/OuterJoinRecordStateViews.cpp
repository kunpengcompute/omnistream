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

#include "OuterJoinRecordStateViews.h"

#include <string>

#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/MapState.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/RowData.h"
#include "table/typeutils/InternalTypeInfo.h"

// ============================================================================
// InputSideHasNoUniqueKey start
// ============================================================================

namespace omnistream {
template <typename K>
OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::InputSideHasNoUniqueKey(
    StreamingRuntimeContext<K>* ctx, const std::string& stateName, InternalTypeInfo* recordType)
{
    if (recordType == nullptr) {
        THROW_LOGIC_EXCEPTION("OuterInputSideHasNoUniqueKey requires a RowData record type");
    }
    auto* descriptor = new MapStateDescriptor<std::shared_ptr<RowData>, UV>(
        stateName, recordType->createTypeSerializer(), new JoinTupleSerializer());
    descriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::TUPLE_INT32_INT32);
    recordState_ = ctx->template getMapState<std::shared_ptr<RowData>, UV>(descriptor);
    this->stateType_ = ctx->getStateType();
    omnistream::checkStateType(this->stateType_, "OuterInputSideHasNoUniqueKey");
    this->joinRecordStateViewType_ = JoinRecordStateView::JoinRecordStateViewType::OUTER_INPUT_SIDE_HAS_NO_UNIQUE_KEY;
}

template <typename K>
void OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::addRecord(const std::shared_ptr<RowData>& record)
{
    addRecord(record, -1);
}

template <typename K>
void OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::addRecord(
    const std::shared_ptr<RowData>& record, int32_t numOfAssociations)
{
    auto existing = recordState_->get(record);
    std::tuple<int32_t, int32_t> value;
    if (existing.has_value()) {
        std::get<0>(value) = std::get<0>(existing.value()) + 1;
        std::get<1>(value) = numOfAssociations;
    } else {
        value = {1, numOfAssociations};
    }
    recordState_->put(record, value);
}

template <typename K>
void OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::updateNumOfAssociations(
    const std::shared_ptr<RowData>& record, int32_t numOfAssociations)
{
    auto existing = recordState_->get(record);
    std::tuple<int32_t, int32_t> value;
    if (existing.has_value()) {
        std::get<0>(value) = std::get<0>(existing.value());
        std::get<1>(value) = numOfAssociations;
    } else {
        // compatible for state ttl
        value = {1, numOfAssociations};
    }
    recordState_->put(record, value);
}

template <typename K>
void OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::retractRecord(const std::shared_ptr<RowData>& record)
{
    auto existing = recordState_->get(record);
    if (existing.has_value()) {
        if (std::get<0>(existing.value()) > 1) {
            std::tuple<int32_t, int32_t> value = {std::get<0>(existing.value()) - 1, std::get<1>(existing.value())};
            recordState_->put(record, value);
        } else {
            recordState_->remove(record);
        }
    }
    // ignore existing == std::nullopt, which means state may be expired
}

template <typename K>
std::unique_ptr<JoinRecordStateView::RecordsAndNumOfAssociationsIterator>
OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::getRecordsAndNumOfAssociations()
{
    class RecordsAndNumOfAssociationsIterator final : public JoinRecordStateView::RecordsAndNumOfAssociationsIterator {
    public:
        explicit RecordsAndNumOfAssociationsIterator(std::unique_ptr<MAP_STATE_TYPE::IteratorV2> iterator)
            : iterator_(std::move(iterator))
        {
        }

        bool hasNext() override
        {
            return iterator_->hasNext() || remainingDuplicates_ > 0;
        }

        std::tuple<std::shared_ptr<RowData>, int32_t> next() override
        {
            if (remainingDuplicates_ > 0) {
                if (std::get<0>(currentTuple_) == nullptr) {
                    THROW_RUNTIME_ERROR(
                        "the currentTuple_ in RecordsAndNumOfAssociationsIterator contains a nullptr record");
                }
                remainingDuplicates_--;
            } else {
                auto& entry = iterator_->next();
                auto entryKey = entry.getKey();
                auto entryValue = entry.getValue();
                if (!entryKey.has_value() || !entryKey.value() || !entryValue.has_value()) {
                    THROW_RUNTIME_ERROR("the state iterator returned an entry without a key or value.");
                }
                currentTuple_ = std::make_tuple(std::move(entryKey.value()), std::get<1>(entryValue.value()));
                remainingDuplicates_ = std::get<0>(entryValue.value()) - 1;
            }
            return currentTuple_;
        }

    private:
        std::unique_ptr<MAP_STATE_TYPE::IteratorV2> iterator_;
        std::tuple<std::shared_ptr<RowData>, int32_t> currentTuple_{};
        int32_t remainingDuplicates_ = 0;
    };

    return std::make_unique<RecordsAndNumOfAssociationsIterator>(recordState_->iteratorV2());
}

template <typename K>
std::unique_ptr<JoinRecordStateView::RecordsIterator>
OuterJoinRecordStateViews::InputSideHasNoUniqueKey<K>::getRecords()
{
    class RecordsIterator final : public JoinRecordStateView::RecordsIterator {
    public:
        explicit RecordsIterator(std::unique_ptr<JoinRecordStateView::RecordsAndNumOfAssociationsIterator> iterator)
            : iterator_(std::move(iterator))
        {
        }

        bool hasNext() override
        {
            return iterator_->hasNext();
        }

        std::shared_ptr<RowData> next() override
        {
            return std::get<0>(iterator_->next());
        }

    private:
        std::unique_ptr<JoinRecordStateView::RecordsAndNumOfAssociationsIterator> iterator_;
    };

    return std::make_unique<RecordsIterator>(getRecordsAndNumOfAssociations());
}

// ============================================================================
// InputSideHasNoUniqueKey end
// ============================================================================

template class OuterJoinRecordStateViews::InputSideHasNoUniqueKey<RowData*>;
template class OuterJoinRecordStateViews::InputSideHasNoUniqueKey<long>;
} // namespace omnistream
