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

#include "org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel.h"
#include <nlohmann/json.hpp>
#include <string>

#include "partition/consumer/OmniLocalChannelReader.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/taskmanager/OmniTask.h"
#include "runtime/state/bridge/OmniLocalInputChannelBridge.h"
#include "jni/bridge/OmniLocalInputChannelBridgeImpl.h"
#include "runtime/partition/consumer/OmniLocalInputChannel.h"

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_doChangeNativeLocalInputChannel(
    JNIEnv* jniEnv, jobject, jlong nativeTaskRef, jstring partitionIdJson)
{
    const char* paritionIdChars = jniEnv->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    jniEnv->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTaskRef);
    return task->changeLocalInputChannelToOriginal(partitionIdPOD);
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_sendMemorySegmentToNative(
    JNIEnv*,
    jobject,
    jlong omniLocalInputChannelRef,
    jlong segmentAddress,
    jint readIndex,
    jint length,
    jint memorySegmentOffset,
    jint sequenceNum,
    jint bufferType)
{
    auto omniInputChannel = reinterpret_cast<omnistream::OmniLocalInputChannel*>(omniLocalInputChannelRef);
    omniInputChannel->notifyOriginalDataAvailable(
        segmentAddress, length, readIndex, sequenceNum, memorySegmentOffset, bufferType);
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_getRecycleBufferAddress(
    JNIEnv*, jobject, jlong omniLocalInputChannelRef)
{
    auto omniInputChannel = reinterpret_cast<omnistream::OmniLocalInputChannel*>(omniLocalInputChannelRef);
    return omniInputChannel->GetRecycleBufferAddress();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_registerJavaOmniLocalInputChannel(
    JNIEnv* env, jobject thiz, jlong omniLocalInputChannelRef)
{
    auto omniInputChannel = reinterpret_cast<omnistream::OmniLocalInputChannel*>(omniLocalInputChannelRef);
    std::shared_ptr<OmniLocalInputChannelBridgeImpl> bridge = std::make_shared<OmniLocalInputChannelBridgeImpl>();
    bridge->RegisterJavaOmniLocalInputChannel(env, thiz);
    omniInputChannel->SetOmniLocalInputChannelBridge(bridge);
}
