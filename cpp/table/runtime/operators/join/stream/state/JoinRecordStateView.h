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

#include <memory>

#include "core/api/common/state/State.h"
#include "utils/Iterator.h"

class RowData;

namespace omnistream {
class JoinRecordStateView {
public:
    using RecordsIterator = omnistream::utils::Iterator<std::shared_ptr<RowData>>;
    using RecordsAndNumOfAssociationsIterator =
        omnistream::utils::Iterator<std::tuple<std::shared_ptr<RowData>, int32_t>>;

    enum JoinRecordStateViewType {
        UNKNOWN,

        JOIN_KEY_CONTAINS_UNIQUE_KEY,
        INPUT_SIDE_HAS_UNIQUE_KEY,
        INPUT_SIDE_HAS_NO_UNIQUE_KEY,

        OUTER_JOIN_KEY_CONTAINS_UNIQUE_KEY,
        OUTER_INPUT_SIDE_HAS_UNIQUE_KEY,
        OUTER_INPUT_SIDE_HAS_NO_UNIQUE_KEY
    };

    static bool isOuterJoinRecordStateViewType(JoinRecordStateViewType type)
    {
        if (type == OUTER_JOIN_KEY_CONTAINS_UNIQUE_KEY || type == OUTER_INPUT_SIDE_HAS_UNIQUE_KEY ||
            type == OUTER_INPUT_SIDE_HAS_NO_UNIQUE_KEY) {
            return true;
        }
        return false;
    }

    virtual ~JoinRecordStateView() = default;

    [[nodiscard]] virtual JoinRecordStateViewType getJoinRecordStateViewType() const
    {
        return joinRecordStateViewType_;
    }

    [[nodiscard]] virtual omnistream::StateType getStateType() const
    {
        return stateType_;
    }

    virtual void addRecord(const std::shared_ptr<RowData>& record) = 0;

    virtual void retractRecord(const std::shared_ptr<RowData>& record) = 0;

    virtual std::unique_ptr<RecordsIterator> getRecords() = 0;

protected:
    JoinRecordStateViewType joinRecordStateViewType_ = UNKNOWN;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
};
} // namespace omnistream
