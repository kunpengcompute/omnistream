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

#ifndef MERGINGWINDOWASSIGNER_H
#define MERGINGWINDOWASSIGNER_H

#pragma once

#include <unordered_set>
#include <set>
#include "WindowAssigner.h"

template <typename W>
class MergingWindowAssigner : public WindowAssigner<W> {
public:
    using MergeResultCollector = std::unordered_map<W, std::unordered_set<W>*>;

    virtual void mergeWindows(const W& newWindow, std::set<W>* sortedWindows, MergeResultCollector& callback) = 0;
};

#endif
