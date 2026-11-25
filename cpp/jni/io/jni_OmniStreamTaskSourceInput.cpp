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

#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput.h"
#include "streaming/runtime/tasks/StreamTask.h"
#include "streaming/api/operators/SourceOperator.h"

JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_emitNextNative(
    JNIEnv*, jclass, jlong nativeStreamTaskRef)
{
    auto* streamTask = reinterpret_cast<omnistream::datastream::StreamTask*>(nativeStreamTaskRef);
    auto* sourceOperator = reinterpret_cast<SourceOperator<>*>(streamTask->getMainOperator());
    auto dataOutput = sourceOperator->getDataStreamOutput();
    DataInputStatus status = sourceOperator->emitNext(dataOutput);

    streamTask->resetOutputBufferAndRecordWriter();
    return static_cast<jint>(status);
}

JNIEXPORT jboolean JNICALL
Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_getSourceReaderAvailableNative(
    JNIEnv*, jclass, jlong nativeStreamTaskRef)
{
    return static_cast<jboolean>(true);
}
