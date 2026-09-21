/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "org_apache_flink_runtime_io_network_partition_consumer_OmniRemoteInputChannel.h"
#include <nlohmann/json.hpp>
#include <string>

#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/taskmanager/OmniTask.h"

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_io_network_partition_consumer_OmniRemoteInputChannel_doAddExtraInfoToNativeRemoteInputChannel
        (JNIEnv* jniEnv, jobject, jlong nativeTaskRef, jstring partitionIdJson)
{
    const char* paritionIdChars = jniEnv->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    jniEnv->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTaskRef);
    return task->addExtraInfoToRemoteInputChannel(partitionIdPOD);
}