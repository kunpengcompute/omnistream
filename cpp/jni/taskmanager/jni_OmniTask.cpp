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

#include <stdexcept>
#include <thread>
#include <taskmanager/OmniTask.h>
#include "common.h"
#include <bridge/OmniTaskBridgeImpl2.h>
#include "com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher.h"
#include "checkpoint/SavepointType.h"
#include <bridge/RemoteDataFetcherBridgeImpl.h>
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    setupStreamTaskBeforeInvoke
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_setupStreamTaskBeforeInvoke(
    JNIEnv* jniEnv, jobject thiz, jlong nativeTask, jstring className)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);

    const char* utf8String = jniEnv->GetStringUTFChars(className, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }

    std::string clsName(utf8String);

    jniEnv->ReleaseStringUTFChars(className, utf8String); // VERY IMPORTANT: Release the string
    LOG("class name : " << clsName);

    long streamTaskAddress = task->setupStreamTask(clsName);
    return streamTaskAddress;
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunNativeTask(
    JNIEnv*, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->doRun(streamTaskAddress);
    return 1;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunInvokeNativeTask(
    JNIEnv*, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        task->DoRunInvoke(streamTaskAddress);
    } catch (const std::exception& e) {
        ERROR_RELEASE("std::exception during DoRunInvoke: " << e.what());
        return 1;
    } catch (...) {
        ERROR_RELEASE("unknown error during DoRunInvoke");
        return 1;
    }

    return 0;
}

/*
 * Hands ownership of the task to the native ResultPartitionManager. Java calls this only after it
 * has unregistered every metric that reads through nativeTaskRef, because the task may be deleted
 * during this call, or later on a netty thread once the last consumer releases its view.
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_notifyNativeTaskRunFinished(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    // May delete the task, so nothing may touch `task` after this call.
    task->NotifyRunFinished();
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunRestoreNativeTask(
    JNIEnv*, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        task->DoRunRestore(streamTaskAddress);
    } catch (const std::exception& e) {
        ERROR_RELEASE("std::exception during doRunRestore: " << e.what());
        return 1;
    } catch (...) {
        ERROR_RELEASE("unknown error during doRunRestore");
        return 1;
    }

    return 0;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doDeleteNativeTask(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    delete task;
    return 1;
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_dispatchOperatorEvent(
    JNIEnv* env, jobject, jlong nativeTask, jstring operatorId, jstring eventDesc)
{
    auto* task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);

    const char* eventCharArray = (env)->GetStringUTFChars(eventDesc, nullptr);
    std::string eventString(eventCharArray);
    (env)->ReleaseStringUTFChars(eventDesc, eventCharArray);

    const char* operatorIdCharArray = (env)->GetStringUTFChars(operatorId, nullptr);
    std::string operatorIdString(operatorIdCharArray);
    (env)->ReleaseStringUTFChars(operatorId, operatorIdCharArray);

    task->dispatchOperatorEvent(operatorIdString, eventString);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_notifyChannelToOmni(
    JNIEnv* env, jobject, jlong nativeTaskRef, jstring partitionIdJson)
{
    const char* paritionIdChars = (env)->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    (env)->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTaskRef);
    task->notifyChannelToOmni(partitionIdPOD);
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_notifyRemoteDataAvailable(
    JNIEnv*,
    jobject,
    jlong nativeTask,
    jint inputGateIndex,
    jint channelIndex,
    jlong bufferAddress,
    jint bufferLength,
    jint readIndex,
    jint memorySegmentOffset,
    jint sequenceNumber,
    jboolean isBuffer,
    jint bufferType)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyRemoteDataAvailable(
        inputGateIndex,
        channelIndex,
        bufferAddress,
        bufferLength,
        readIndex,
        memorySegmentOffset,
        sequenceNumber,
        isBuffer,
        bufferType);
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_createNativeTaskMetricGroup(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    auto taskMetricGroup = task->createTaskMetricGroup();
    return reinterpret_cast<long>(taskMetricGroup.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_cancelTask(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        task->cancel();
        return 0;
    } catch (const std::exception& e) {
        ERROR_RELEASE("std::exception during native task cancellation: " << e.what());
    } catch (...) {
        ERROR_RELEASE("unknown error during native task cancellation");
    }
    return 1;
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_getRecycleBufferAddress(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    return task->GetRecycleBufferAddress();
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_triggerCheckpointCpp(
    JNIEnv* jniEnv,
    jobject,
    jlong nativeTask,
    jlong checkpointID,
    jlong checkpointTimestamp,
    jstring checkpointoptionJson)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    if (!task) {
        INFO_RELEASE("Error OmniTask_triggerCheckpointCpp task is null");
        THROW_LOGIC_EXCEPTION("OmniTask_triggerCheckpointCpp task is null");
    }
    const char* checkpointStr = jniEnv->GetStringUTFChars(checkpointoptionJson, nullptr);
    nlohmann::json checkpointoptionJsonStr = json::parse(checkpointStr);
    jniEnv->ReleaseStringUTFChars(checkpointoptionJson, checkpointStr);
    std::shared_ptr<CheckpointOptions> configuredOptions(CheckpointOptions::FromJson(checkpointoptionJsonStr));
    task->triggerCheckpointBarrier(checkpointID, checkpointTimestamp, configuredOptions);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_abortCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong checkpointId, jlong latestCompletedCheckpointId)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointAborted(checkpointId, latestCompletedCheckpointId);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_completeCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong checkpointId, jlong inputState)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointComplete(checkpointId, inputState);
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    subsumedCpp
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_subsumedCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong latestCompletedCheckpointId)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointSubsumed(latestCompletedCheckpointId);
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_registerRemoteDataFetcherToNative(
    JNIEnv* env, jobject thiz, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    auto bridgeBase = task->GetRemoteDataFetcherBridge();
    if (!bridgeBase) {
        LOG("GetRemoteDataFetcherBridge returned null");
        return;
    }
    std::shared_ptr<RemoteDataFetcherBridgeImpl> remote =
        std::static_pointer_cast<RemoteDataFetcherBridgeImpl>(bridgeBase);
    remote->SetJavaRemoteDataFetcher(env, thiz);
}

extern "C" JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_closeNativeRocksDBSharedResources(
    JNIEnv* env, jobject, jlong nativeTask)
{
    try {
        reinterpret_cast<omnistream::OmniTask*>(nativeTask)->closeRocksDBSharedResources();
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), e.what());
    } catch (...) {
        env->ThrowNew(
            env->FindClass("java/lang/IllegalStateException"), "Unable to close native RocksDB shared resources");
    }
}
