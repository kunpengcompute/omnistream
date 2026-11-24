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
#include <type_traits>
#include "InternalWindowProcessFunction.h"
#include "MergingWindowSet.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "table/runtime/operators/window/WindowOperator.h"

template <typename K, typename W>
class MergingWindowProcessFunction : public InternalWindowProcessFunction<K, W> {
    static_assert(std::is_base_of_v<Window, W>, "W must inherit from Window");

public:
    MergingWindowProcessFunction(
        MergingWindowAssigner<W>* windowAssigner,
        NamespaceAggsHandleFunctionBase<W>* windowAggregator,
        TypeSerializer* windowSerializer,
        int64_t allowedLateness)
        : InternalWindowProcessFunction<K, W>(windowAssigner, windowAggregator, allowedLateness),
          windowAssigner(windowAssigner),
          windowSerializer(windowSerializer)
    {
    }

    void open(Context<K, W>* ctx) override;

    std::vector<W> assignStateNamespace(RowData* keyRowData, int64_t timestamp) override;

    std::vector<W> assignActualWindows(RowData* inputRow, int64_t timestamp) override;

    void prepareAggregateAccumulatorForEmit(const W& window) override;

    void cleanWindowIfNeeded(const W& window, int64_t currentTime) override;

private:
    MergingWindowAssigner<W>* windowAssigner{};
    std::unique_ptr<MergingWindowSet<K, W>> mergingWindows;
    TypeSerializer* windowSerializer{};
    std::vector<W> reuseActualWindows;
};
