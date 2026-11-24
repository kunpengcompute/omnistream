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

#ifndef FLINK_TNEL_KEYED_PROCESS_FUNCTION_H
#define FLINK_TNEL_KEYED_PROCESS_FUNCTION_H

#include "functions/AbstractRichFunction.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/JoinedRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "api/common/state/ValueState.h"
#include "streaming/api/TimerService.h"

template <typename K, typename IN, typename O>
class KeyedProcessFunction : public AbstractRichFunction {
public:
    class Context {
    public:
        virtual ~Context() = default;
        virtual long timestamp() const = 0;
        virtual omnistream::streaming::TimerService* timerService() = 0;
        virtual K getCurrentKey() const = 0;
        virtual void setCurrentKey(K key) = 0;
    };

    class OnTimerContext : public Context {
    public:
        virtual ~OnTimerContext() = default;
        virtual K getCurrentKey() const override = 0;
    };
    // virtual void open(OpenContext* openContext) = 0;

    virtual void processElement(IN input, Context* ctx, TimestampedCollector* out) = 0;
    virtual void processBatch(omnistream::VectorBatch* inputBatch, Context& ctx, TimestampedCollector& out) = 0;
    virtual JoinedRowData* getResultRow() = 0;

    ~KeyedProcessFunction() = default;
    virtual ValueState<K>* getValueState() = 0;
};

#endif // FLINK_TNEL_KEYED_PROCESS_FUNCTION_H
