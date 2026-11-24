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

#ifndef SIMPLE_COUNTER_H
#define SIMPLE_COUNTER_H
#include <atomic>

#include "Counter.h"

namespace omnistream {
/**
 * Counter whose address is handed to the Java side as a raw jlong (see
 * OmniMetricHelper.createNativeSimpleCounter). The shared_ptr lives in the metric group, which
 * dies with its task, but the Java metric object keeps the address and Flink's ViewUpdater goes
 * on polling it on a timer -- so the JNI accessor can be called after this object is gone.
 *
 * Every live instance registers itself, and IsLive lets the JNI boundary check an address before
 * dereferencing it. Only construction, destruction and the JNI accessor touch the registry; Inc
 * and Dec are on the per-record hot path and stay lock free.
 */
class SimpleCounter : public Counter {
public:
    SimpleCounter();
    ~SimpleCounter() override;
    void Inc() override;
    void Inc(long n) override;
    void Dec() override;
    void Dec(long n) override;
    long GetCount() override;

    // True only while `counter` points at a constructed, not-yet-destroyed SimpleCounter.
    static bool IsLive(const SimpleCounter* counter);

private:
    long count;
};
} // namespace omnistream
#endif // SIMPLE_COUNTER_H
