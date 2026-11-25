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

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2.h"
#include "runtime/taskmanager/OmniTask.h"

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2_dispatchOperatorEvent(
    JNIEnv* env, jclass, jlong nativeTask, jstring operatorId, jstring eventDesc)
{
    auto* task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);

    const char* eventCharArray = env->GetStringUTFChars(eventDesc, nullptr);
    std::string eventString(eventCharArray);
    env->ReleaseStringUTFChars(eventDesc, eventCharArray);

    const char* operatorIdCharArray = env->GetStringUTFChars(operatorId, nullptr);
    std::string operatorIdString(operatorIdCharArray);
    env->ReleaseStringUTFChars(operatorId, operatorIdCharArray);

    task->dispatchOperatorEvent(operatorIdString, eventString);
}
