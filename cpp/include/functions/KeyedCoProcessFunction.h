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

#ifndef OMNISTREAM_KEYEDCOPROCESSFUNCTION_H
#define OMNISTREAM_KEYEDCOPROCESSFUNCTION_H
#include "AbstractRichFunction.h"
#include "functions/Collector.h"
#include "streaming/api/TimerService.h"
#include "streaming/api/TimeDomain.h"

/**
 * K: such as Object
 * IN1: such as Object*
 * IN2: such as Object*
 * OUT: such as Object*
 * */
template <typename K, typename IN1, typename IN2, typename OUT>
class KeyedCoProcessFunction : public AbstractRichFunction {
public:
    virtual ~KeyedCoProcessFunction() = default;

    /**
     * Information available in an invocation of {@link #processElement1(Object, Context,
     * Collector)}/ {@link #processElement2(Object, Context, Collector)} or {@link #onTimer(long,
     * OnTimerContext, Collector)}.
     */
    class Context : public Object {
    public:
        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        virtual long timestamp() = 0;

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        virtual void output(Object* value) = 0;

        /** Get key of the element being processed. */
        virtual K getCurrentKey() = 0;

    public:
        /** A {@link TimerService} for querying time and registering timers. */
        virtual omnistream::streaming::TimerService* timerService() = 0;
    };

    /**
     * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
     */
    class OnTimerContext : public Context {
    public:
        /** The {@link TimeDomain} of the firing timer. */
        virtual TimeDomain timeDomain() = 0;
    };
    /**
     * This method is called for each element in the first of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    virtual void processElement1(IN1 value, Context* ctx, Collector* out) = 0;

    /**
     * This method is called for each element in the second of the connected streams.
     *
     * <p>This function can output zero or more elements using the {@link Collector} parameter and
     * also update internal state or set timers using the {@link Context} parameter.
     *
     * @param value The stream element
     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
     *     timers and querying the time. The context is only valid during the invocation of this
     *     method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception The function may throw exceptions which cause the streaming program to fail
     *     and go into recovery.
     */
    virtual void processElement2(IN2 value, Context* ctx, Collector* out) = 0;

    /**
     * Called when a timer set using {@link TimerService} fires.
     *
     * @param timestamp The timestamp of the firing timer.
     * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
     *     querying the {@link TimeDomain} of the firing timer and getting a {@link TimerService}
     *     for registering timers and querying the time. The context is only valid during the
     *     invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    virtual void onTimer(long timestamp, OnTimerContext* ctx, Collector* out) {};
};

template <typename K, typename IN1, typename IN2, typename OUT>
using KeyedCoProcessFunctionUnique = std::unique_ptr<KeyedCoProcessFunction<K, IN1, IN2, OUT>>;
#endif // OMNISTREAM_KEYEDCOPROCESSFUNCTION_H
