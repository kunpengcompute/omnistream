/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this
 * software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:

 * *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES
 * OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT
 * FOR A PARTICULAR PURPOSE.
 */

#pragma once

#include <optional>
#include "TimestampData.h"

class RowData;

using getFieldByPosFn = void* (RowData::*)(int pos);

class FieldGetter {
public:
    FieldGetter(int fieldPos, getFieldByPosFn getFieldByPos);
    FieldGetter(int fieldPos, bool preciseTimestamp);

    void* getFieldOrNull(RowData* row);

private:
    int fieldPos_;
    getFieldByPosFn getFieldByPos_ = nullptr;
    bool timestampGetter_ = false;
    bool preciseTimestamp_ = false;
    std::optional<TimestampData> timestampValue_;
};
