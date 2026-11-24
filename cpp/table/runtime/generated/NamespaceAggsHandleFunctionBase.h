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

class RowData;
class StateDataViewStore;

template <typename N>
class NamespaceAggsHandleFunctionBase {
public:
    explicit NamespaceAggsHandleFunctionBase(int32_t accumulatorArity) : accumulatorArity_(accumulatorArity)
    {
    }

    virtual ~NamespaceAggsHandleFunctionBase() = default;

    // 初始化方法
    virtual void open(StateDataViewStore* store) = 0;

    // 设置当前累加器
    virtual void setAccumulators(N namespace_val, RowData* accumulators) = 0;

    // 累积输入值
    virtual void accumulate(RowData* input_row) = 0;

    // 回撤输入值
    virtual void retract(RowData* input_row) = 0;

    // 合并其他累加器
    virtual void merge(N namespace_val, RowData* other_acc) = 0;

    // 创建初始累加器
    virtual RowData* createAccumulators() = 0;

    // 获取当前累加器
    virtual RowData* getAccumulators() = 0;

    // 清理命名空间
    virtual void cleanup(N namespace_val) = 0;

    // 关闭资源
    virtual void close() = 0;

protected:
    int32_t accumulatorArity_;
};
