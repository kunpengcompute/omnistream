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

#include <streaming/runtime/tasks/omni/OmniSourceStreamTask.h>
#include "common.h"

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2.h"
#include "streaming/runtime/tasks/StreamTask.h"

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_runStreamSource(
    JNIEnv* env, jobject obj, jlong omniStreamTaskRef)
{
    LOG("runStreamSource at " << reinterpret_cast<long>(omniStreamTaskRef));
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    auto source = reinterpret_cast<omnistream::StreamSource<Object>*>(pTask->getMainOperator());
    jclass task = env->GetObjectClass(obj);
    auto methodID = env->GetMethodID(task, "process", "()V");
    source->setProcessArgs(methodID, env, obj);
    source->run();
    return 0L;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_cancelStreamSource(
    JNIEnv*, jobject, jlong omniStreamTaskRef)
{
    LOG("cancelStreamSource at " << reinterpret_cast<long>(omniStreamTaskRef));
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    auto source = reinterpret_cast<omnistream::StreamSource<Object>*>(pTask->getMainOperator());
    source->cancel();
    return 0L;
}

JNIEXPORT void JNICALL
Java_org_apache_flink_streaming_runtime_tasks_OmniSourceStreamTask_resetOutputBufferAndRecordWriter(
    JNIEnv*, jobject, jlong omniStreamTaskRef)
{
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    pTask->resetOutputBufferAndRecordWriter();
}
