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

#ifndef WINDOWASSIGNER_H
#define WINDOWASSIGNER_H

#pragma once
#include <cstdint>

#include "table/data/RowData.h"

template <typename W>
class WindowAssigner {
public:
    WindowAssigner() = default;

    virtual ~WindowAssigner() = default;

    virtual void open()
    {
    }

    virtual std::vector<W> assignWindows(const RowData* rowData, int64_t timestamp) = 0;

    virtual bool isEventTime() const = 0;
};

#endif
