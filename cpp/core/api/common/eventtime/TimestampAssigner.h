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

class TimestampAssigner {
public:
    static constexpr int64_t NO_TIMESTAMP = INT64_MIN;

    virtual ~TimestampAssigner() = default;

    virtual long ExtractTimestamp(void* element, long recordTimestamp) = 0;

    virtual int32_t getRowtimeFieldIndex() const = 0;
};

class RecordTimestampAssigner : public TimestampAssigner {
public:
    RecordTimestampAssigner() = default;

    explicit RecordTimestampAssigner(int32_t rowtimeFieldIndex) : rowtimeFieldIndex(rowtimeFieldIndex)
    {
    }

    long ExtractTimestamp(void* element, long recordTimestamp) override
    {
        // TODO: directly return recordTimestamp is not correct
        return recordTimestamp;
    }

    int32_t getRowtimeFieldIndex() const override
    {
        return rowtimeFieldIndex;
    }

private:
    int32_t rowtimeFieldIndex = -1;
};
