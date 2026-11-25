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

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask.h"
#include "common.h"
#include "nlohmann/json.hpp"
#include "streaming/runtime/tasks/StreamTask.h"
#include "runtime/taskmanager/OmniTask.h"

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_createNativeStreamTask(
    JNIEnv* env, jclass clazz, jstring TDDString, jlong statusAddress, jlong nativeTask)
{
    LOG("this is create native task object");
    const char* cStrTDD = (env)->GetStringUTFChars(TDDString, 0);

    LOG("debug tdd is: " + std::string(cStrTDD));

    void* bufferStatus = reinterpret_cast<void*>(statusAddress);

    nlohmann::json tdd = nlohmann::json::parse(cStrTDD);

    LOG("Calling  StreamTask with json " + tdd.dump(2));

    // Update this for setting up environment
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);

    auto* streamTask = new omnistream::datastream::StreamTask(tdd, bufferStatus, task->getRuntimeEnv());
    LOG("After Calling StreamTask with json  " << reinterpret_cast<long>(streamTask));

    env->ReleaseStringUTFChars(TDDString, cStrTDD);

    return reinterpret_cast<long>(streamTask);
};

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_createNativeOmniInputProcessor(
    JNIEnv* env, jclass clazz, jlong omniStreamTaskRef, jstring inputChannelInfo, jint operatorMethodIndicator)
{
    const char* channelInfos = (env)->GetStringUTFChars(inputChannelInfo, 0);

    LOG("channel info is: " + std::string(channelInfos));

    nlohmann::json channelJson = nlohmann::json::parse(channelInfos);
    env->ReleaseStringUTFChars(inputChannelInfo, channelInfos);
    auto* streamTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    auto* processor = streamTask->createOmniInputProcessor(channelJson, operatorMethodIndicator);
    std::cout << "Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_createNativeOmniInputProcessor "
                 "operatorMethodIndicator :"
              << operatorMethodIndicator << std::endl;
    streamTask->addStreamOneInputProcessor(processor);

    return reinterpret_cast<long>(processor);
}

/*
 * Class:     org_apache_flink_streaming_runtime_tasks_OmniStreamTask
 * Method:    removeNativeStreamTask
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_removeNativeStreamTask(
    JNIEnv*, jclass, jlong omniStreamTaskRef)
{
    LOG("Remove Native Stream Task at " << reinterpret_cast<long>(omniStreamTaskRef));
    auto* streamTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    streamTask->cleanUp();
    return 0L;
}
