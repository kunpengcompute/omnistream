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

#include "JoinRecordStateView.h"
#include "common.h"

template <typename UK, typename UV>
class MapState;

template <typename K>
class StreamingRuntimeContext;

class RowData;
class InternalTypeInfo;
namespace omnistream {
class JoinRecordStateViews {
public:
    template <typename K>
    static std::unique_ptr<JoinRecordStateView> create(
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
    class InputSideHasNoUniqueKey : public JoinRecordStateView {
    public:
        using MAP_STATE_TYPE = MapState<std::shared_ptr<RowData>, int32_t>;

        InputSideHasNoUniqueKey(
            StreamingRuntimeContext<K>* ctx, const std::string& stateName, InternalTypeInfo* recordType);

        std::unique_ptr<JoinRecordStateView::RecordsIterator> getRecords() override;

        void addRecord(const std::shared_ptr<RowData>& record) override;

        void retractRecord(const std::shared_ptr<RowData>& record) override;

    private:
        MAP_STATE_TYPE* recordState_;
    };
};

extern template class JoinRecordStateViews::InputSideHasNoUniqueKey<RowData*>;
extern template class JoinRecordStateViews::InputSideHasNoUniqueKey<long>;
} // namespace omnistream
