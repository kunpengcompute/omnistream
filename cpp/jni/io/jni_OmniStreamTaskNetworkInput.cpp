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

#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput.h"
#include "streaming/runtime/io/StreamOneInputProcessor.h"
#include "streaming/runtime/tasks/StreamTask.h"

JNIEXPORT jobject JNICALL
Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput_TNELProcessBuffer(
    JNIEnv* env,
    jobject input,
    jlong omniStreamTaskRef,
    jlong omniInputProcessorRef,
    jlong address,
    jint offset,
    jint numBytes,
    jlong channelInfo)
{
    auto* streamTask = reinterpret_cast<omnistream::datastream::StreamTask*>(omniStreamTaskRef);
    auto* processor = reinterpret_cast<omnistream::datastream::StreamOneInputProcessor*>(omniInputProcessorRef);

    auto buffAddress = reinterpret_cast<uint8_t*>(address);

    LOG(" Inside Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput_TNELProcessBuffer "
        "buffAddress + offset " +
        std::to_string(reinterpret_cast<long>(buffAddress + offset)) + " numBytes " + std::to_string(numBytes));

    auto myClass = env->FindClass("com/huawei/omniruntime/flink/streaming/runtime/io/TNELProcessState");
    auto constructor = env->GetMethodID(myClass, "<init>", "(IJ)V");
    int32_t inputNumber = 0;
    auto status = processor->processInput(buffAddress + offset, numBytes, channelInfo, inputNumber);
    streamTask->resetOutputBufferAndRecordWriter();
    auto myObject = env->NewObject(myClass, constructor, status, inputNumber);
    return myObject;
}
