/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <taskmanager/OmniTask.h>
#include <nlohmann/json.hpp>
#include <string>
#include "common.h"
#include "org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_createNativeCreditBasedSequenceNumberingViewReader(
    JNIEnv* jniEnv,
    jobject input,
    jlong nativeTask,
    jlong resultBufferAddress,
    jstring partitionIdJson,
    jint subPartitionId, jboolean downStreamIsNative)
{
    const char* paritionIdChars = jniEnv->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    jniEnv->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;

    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    return task->createNativeCreditBasedSequenceNumberingViewReader(
        resultBufferAddress, partitionIdPOD, subPartitionId, downStreamIsNative);
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getAvailabilityAndBacklog(
    JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef, jint numCreditsAvailable)
{
    if (creditBasedSequenceNumberingViewReaderRef == -1) {
        return 0;
    }
    auto viewReader = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    return viewReader->getAvailabilityAndBacklog();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_releaseNativeViewReader(
    JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    if (creditBasedSequenceNumberingViewReaderRef == -1) {
        return;
    }
    auto viewReader = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    delete viewReader;
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getNextBuffer(
    JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    return task->getNextBuffer();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_firstDataAvailableNotification(
    JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    if (creditBasedSequenceNumberingViewReaderRef == -1) {
        return;
    }
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    task->notifyDataAvailable();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_resumeConsumption(
    JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    // it is possible OmniCreditBasedSequenceNumberingViewReader is destroyed already
    auto view = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    view->ResumeConsumption();
}
