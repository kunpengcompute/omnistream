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

#include "JoinRecordStateViews.h"

#include <string>

#include "core/api/common/state/State.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/MapState.h"
#include "core/typeutils/LongSerializer.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/RowData.h"
#include "table/typeutils/InternalTypeInfo.h"

// ============================================================================
// InputSideHasNoUniqueKey start
// ============================================================================

namespace omnistream {
template <typename K>
JoinRecordStateViews::InputSideHasNoUniqueKey<K>::InputSideHasNoUniqueKey(
    StreamingRuntimeContext<K>* ctx, const std::string& stateName, InternalTypeInfo* recordType)
{
    if (recordType == nullptr) {
        THROW_LOGIC_EXCEPTION("InputSideHasNoUniqueKey requires a RowData record type");
    }
    auto* descriptor = new MapStateDescriptor<std::shared_ptr<RowData>, int32_t>(
        stateName, recordType->createTypeSerializer(), new IntSerializer());
    descriptor->setKeyValueBackendTypeId(BackendDataType::SHARED_ROW_BK, BackendDataType::INT_BK);
    recordState_ = ctx->template getMapState<std::shared_ptr<RowData>, int32_t>(descriptor);
    this->stateType_ = ctx->getStateType();
    omnistream::checkStateType(this->stateType_, "InputSideHasNoUniqueKey");
    this->joinRecordStateViewType_ = JoinRecordStateView::JoinRecordStateViewType::INPUT_SIDE_HAS_NO_UNIQUE_KEY;
}

template <typename K>
void JoinRecordStateViews::InputSideHasNoUniqueKey<K>::addRecord(const std::shared_ptr<RowData>& record)
{
    auto existing = recordState_->get(record);
    int32_t value;
    if (existing.has_value()) {
        value = existing.value() + 1;
    } else {
        value = 1;
    }
    recordState_->put(record, value);
}

template <typename K>
void JoinRecordStateViews::InputSideHasNoUniqueKey<K>::retractRecord(const std::shared_ptr<RowData>& record)
{
    auto existing = recordState_->get(record);
    if (existing.has_value()) {
        if (existing.value() > 1) {
            recordState_->put(record, existing.value() - 1);
        } else {
            recordState_->remove(record);
        }
    }
    // ignore existing == std::nullopt, which means state may be expired
}

template <typename K>
std::unique_ptr<JoinRecordStateView::RecordsIterator> JoinRecordStateViews::InputSideHasNoUniqueKey<K>::getRecords()
{
    class RecordsIterator final : public JoinRecordStateView::RecordsIterator {
    public:
        explicit RecordsIterator(std::unique_ptr<MAP_STATE_TYPE::IteratorV2> iterator) : iterator_(std::move(iterator))
        {
        }

        bool hasNext() override
        {
            return iterator_->hasNext() || remainingDuplicates_ > 0;
        }

        std::shared_ptr<RowData> next() override
        {
            if (remainingDuplicates_ > 0) {
                if (currentRecord_ == nullptr) {
                    THROW_RUNTIME_ERROR("currentRecord_ in RecordsIterator is nullptr");
                }
                remainingDuplicates_--;
            } else {
                auto& entry = iterator_->next();
                auto entryKey = entry.getKey();
                auto entryValue = entry.getValue();
                if (!entryKey.has_value() || !entryKey.value() || !entryValue.has_value()) {
                    THROW_RUNTIME_ERROR("the state iterator returned an entry without a key or value.");
                }
                currentRecord_ = std::move(entryKey.value());
                remainingDuplicates_ = entryValue.value() - 1;
            }
            return currentRecord_;
        }

    private:
        std::unique_ptr<MAP_STATE_TYPE::IteratorV2> iterator_;
        std::shared_ptr<RowData> currentRecord_{};
        int32_t remainingDuplicates_ = 0;
    };

    return std::make_unique<RecordsIterator>(recordState_->iteratorV2());
}

// ============================================================================
// InputSideHasNoUniqueKey end
// ============================================================================

template class JoinRecordStateViews::InputSideHasNoUniqueKey<RowData*>;
template class JoinRecordStateViews::InputSideHasNoUniqueKey<long>;
} // namespace omnistream
