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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>

#include "OuterJoinRecordStateView.h"
#include "core/api/common/state/State.h"
#include "common.h"

template <typename UK, typename UV>
class MapState;

template <typename K>
class StreamingRuntimeContext;

class RowData;
class InternalTypeInfo;
namespace omnistream {
class OuterJoinRecordStateViews {
public:
    template <typename K>
    static std::unique_ptr<OuterJoinRecordStateView> create(
        StreamingRuntimeContext<K>* ctx, const std::string& stateName, InternalTypeInfo* recordType)
    {
        // todo: 这里校验有问题
        if (stateName.find("JoinKeyContainsUniqueKey") != std::string::npos ||
            stateName.find("HasUnique") != std::string::npos) {
            NOT_IMPL_EXCEPTION;
        }
        return std::make_unique<InputSideHasNoUniqueKey<K>>(ctx, stateName, recordType);
    }

    template <typename K>
    class InputSideHasNoUniqueKey : public OuterJoinRecordStateView {
    public:
        using UV = std::tuple<int32_t, int32_t>; // duplicate count, number of associations
        using MAP_STATE_TYPE = MapState<std::shared_ptr<RowData>, UV>;

        InputSideHasNoUniqueKey(
            StreamingRuntimeContext<K>* ctx, const std::string& stateName, InternalTypeInfo* recordType);

        void addRecord(const std::shared_ptr<RowData>& record) override;

        void retractRecord(const std::shared_ptr<RowData>& record) override;

        std::unique_ptr<JoinRecordStateView::RecordsIterator> getRecords() override;

        void addRecord(const std::shared_ptr<RowData>& record, int32_t numOfAssociations) override;

        void updateNumOfAssociations(const std::shared_ptr<RowData>& record, int32_t numOfAssociations) override;

        std::unique_ptr<JoinRecordStateView::RecordsAndNumOfAssociationsIterator> getRecordsAndNumOfAssociations()
            override;

    private:
        MAP_STATE_TYPE* recordState_;
    };
};

extern template class OuterJoinRecordStateViews::InputSideHasNoUniqueKey<RowData*>;
extern template class OuterJoinRecordStateViews::InputSideHasNoUniqueKey<long>;
} // namespace omnistream
