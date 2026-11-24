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

#ifndef FLINK_TNEL_RICHFLATMAPFUNCTION_H
#define FLINK_TNEL_RICHFLATMAPFUNCTION_H

#include "FlatMapFunction.h"
#include "AbstractRichFunction.h"

template <typename T>
class RichFlatMapFunction : public FlatMapFunction<T>, public AbstractRichFunction {
public:
    virtual void flatMap(T* obj, Collector* collector) = 0;
};

template <typename T>
using RichFlatMapFunctionUnique = std::unique_ptr<RichFlatMapFunction<T>>;

#endif // FLINK_TNEL_RICHFLATMAPFUNCTION_H
