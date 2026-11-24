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

#include "com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView.h"
#include <nlohmann/json.hpp>
#include <string>
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/taskmanager/OmniTask.h"
#include "runtime/partition/consumer/OmniLocalChannelReader.h"
JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_createLocalChannelReader(
    JNIEnv* jniEnv, jobject, jlong nativeTask, jstring partitionIdJson, jint subPartitionId, jlong returnDataAddress)
{
    const char* paritionIdChars = jniEnv->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    jniEnv->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;

    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    return task->createOmniLocalChannelReader(partitionIdPOD, subPartitionId, returnDataAddress);
}

JNIEXPORT jint JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_getNativeNextBuffer(
    JNIEnv*, jobject, jlong nativeLocalChannelReaderRef)
{
    auto omniLocalChannelReader = reinterpret_cast<omnistream::OmniLocalChannelReader*>(nativeLocalChannelReaderRef);
    return omniLocalChannelReader->getNextBuffer();
}

JNIEXPORT jboolean JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_doCheckIfDataAvailable(
    JNIEnv*, jobject, jlong nativeLocalChannelReaderRef)
{
    auto omniLocalChannelReader = reinterpret_cast<omnistream::OmniLocalChannelReader*>(nativeLocalChannelReaderRef);
    return omniLocalChannelReader->checkIfDataAvailable();
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_releaseNativeResource(
    JNIEnv*, jobject, jlong nativeLocalChannelReaderRef)
{
    auto omniLocalChannelReader = reinterpret_cast<omnistream::OmniLocalChannelReader*>(nativeLocalChannelReaderRef);
    omniLocalChannelReader->ReleaseAllResources();
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_resumeConsumption(
    JNIEnv*, jobject, jlong nativeLocalChannelReaderRef)
{
    auto omniLocalChannelReader = reinterpret_cast<omnistream::OmniLocalChannelReader*>(nativeLocalChannelReaderRef);
    omniLocalChannelReader->ResumeConsumption();
}
