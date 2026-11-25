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

#ifndef FLINK_TNEL_PROCESSFUNCTION_H
#define FLINK_TNEL_PROCESSFUNCTION_H
#include "functions/Collector.h"
#include "streaming/api/TimerService.h"
#include "streaming/api/TimeDomain.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/utils/OutputTag.h"
#include "functions/AbstractRichFunction.h"

/**
 * A function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)} is
 * invoked. This can produce zero or more elements as output. Implementations can also query the
 * time and set timers through the provided {@link Context}. For firing timers {@link #onTimer(long,
 * OnTimerContext, Collector)} will be invoked. This can again produce zero or more elements as
 * output and register further timers.
 *
 * @param <I> Type of the input elements. such as Object*, VectorBatch*
 * @param <O> Type of the output elements. such as Object*, VectorBatch*
 * */
template <typename I, typename O>
class ProcessFunction : public AbstractRichFunction {
public:
    class Context : public Object {
        virtual int64_t timestamp() = 0;
        virtual omnistream::streaming::TimerService* timerService() = 0;

        virtual void Output(OutputTag* outputTag, Object* value) = 0;
    };
    class OnTimerContext : public Context {
        virtual TimeDomain timeDomain() = 0;
    };

    virtual ~ProcessFunction() = default;

    virtual void processElement(Object* value, Context* cxt, Collector* collector)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void processBatch(omnistream::VectorBatch* value, Context* cxt, Collector* collector)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void onTimer(int64_t timestamp, class OnTimerContext* ctx, Collector* collector) {};
};

template <typename I, typename O>
using ProcessFunctionUnique = std::unique_ptr<ProcessFunction<I, O>>;

#endif // FLINK_TNEL_PROCESSFUNCTION_H
