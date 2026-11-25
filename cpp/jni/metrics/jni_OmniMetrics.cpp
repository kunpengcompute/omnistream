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

#include <jni.h>
#include "org_apache_flink_metrics_SimpleCounter.h"
#include "runtime/metrics/SimpleCounter.h"

JNIEXPORT jlong JNICALL Java_org_apache_flink_metrics_SimpleCounter_getNativeCounterNew(JNIEnv*, jobject, jlong nativeR)
{
    // Flink's ViewUpdater polls counters on a timer and holds this address indefinitely, but the
    // counter dies with its metric group when the task is destroyed. Without this check the poll
    // dereferences freed memory -- observed as a TaskManager SIGSEGV in ViewUpdaterTask.run().
    // A dead counter reports 0: its task is gone, so there is no count left to report.
    auto counter = reinterpret_cast<omnistream::SimpleCounter*>(nativeR);
    if (!omnistream::SimpleCounter::IsLive(counter)) {
        return 0;
    }
    return counter->GetCount();
}
