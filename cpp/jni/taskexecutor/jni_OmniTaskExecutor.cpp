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

#include <jni.h>
#include <bridge/OmniTaskBridgeImpl2.h>
#include <bridge/TaskOperatorEventGatewayBridgeImpl.h>
#include <bridge/TaskStateManagerBridgeImpl.h>
#include <bridge/RemoteDataFetcherBridgeImpl.h>

#include <nlohmann/json.hpp>
#include "com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor.h"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "common.h"
#include "runtime/executiongraph/JobInformationPOD.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include "runtime/executiongraph/descriptor/TaskDeploymentDescriptorPOD.h"
#include "state/bridge/TaskOperatorEventGatewayBridge.h"
#include "runtime/partition/consumer/RemoteDataFetcherBridge.h"

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    createNativeTaskExecutor
 * Signature: (Ljava/lang/String;J)J
 */
JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_createNativeTaskExecutor(
    JNIEnv* jnienv, jobject thiz, jstring taskExecutorConfiguration, jlong nativeTaskManagerServiceAddress)
{
    auto taskManagerServices = reinterpret_cast<omnistream::TaskManagerServices*>(nativeTaskManagerServiceAddress);
    auto taskManagerServicesWrap = std::shared_ptr<omnistream::TaskManagerServices>(taskManagerServices);
    omnistream::OmniTaskExecutor* omniTaskExecutor = new omnistream::OmniTaskExecutor(taskManagerServicesWrap);
    return reinterpret_cast<jlong>(omniTaskExecutor);
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    submitTaskNative
 * Signature: (JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_submitTaskNative(
    JNIEnv* jniEnv, jobject thiz, jlong nativeTaskExecutorAddress, jstring jobjson, jstring taskjson, jstring tddjson)
try {
    const char* jobString = jniEnv->GetStringUTFChars(jobjson, nullptr);
    std::string jobInfoString(jobString);
    jniEnv->ReleaseStringUTFChars(jobjson, jobString);

    const char* taskString = jniEnv->GetStringUTFChars(taskjson, nullptr);
    std::string taskInfoString(taskString);
    jniEnv->ReleaseStringUTFChars(taskjson, taskString);

    const char* tddString = jniEnv->GetStringUTFChars(tddjson, nullptr);
    std::string taskDDString(tddString);
    jniEnv->ReleaseStringUTFChars(tddjson, tddString);

    LOG("jobjson " << jobInfoString);
    LOG("taskjson " << taskInfoString);
    LOG("tddjson " << taskDDString);

    nlohmann::json job = nlohmann::json::parse(jobInfoString);
    omnistream::JobInformationPOD jobInfo = job;
    nlohmann::json task = nlohmann::json::parse(taskInfoString);
    omnistream::TaskInformationPOD taskInfo = task;
    nlohmann::json tdd = nlohmann::json::parse(taskDDString);
    omnistream::TaskDeploymentDescriptorPOD tddinfo = tdd;
    taskInfo.validateInputGateCount(tddinfo.getInputGates().size());

    auto omniTaskExecutor = reinterpret_cast<omnistream::OmniTaskExecutor*>(nativeTaskExecutorAddress);
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge = std::make_shared<RemoteDataFetcherBridgeImpl>();

    auto nativeTask =
        omniTaskExecutor->submitTask(jobInfo, taskInfo, tddinfo, nullptr, nullptr, nullptr, remoteDataFetcherBridge);
    return reinterpret_cast<jlong>(nativeTask);
} catch (const std::exception& error) {
    if (!jniEnv->ExceptionCheck()) {
        jclass exceptionClass = jniEnv->FindClass("java/lang/IllegalArgumentException");
        if (exceptionClass != nullptr) {
            jniEnv->ThrowNew(exceptionClass, error.what());
            jniEnv->DeleteLocalRef(exceptionClass);
        }
    }
    return -1;
}
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    submitTaskNativeWithCheckpointing
 * Signature:
 * (JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Lcom/huawei/omniruntime/flink/runtime/state/TaskStateManagerWrapper;)J
 */

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_submitTaskNativeWithCheckpointing(
    JNIEnv* jniEnv,
    jobject thiz,
    jlong nativeTaskExecutorAddress,
    jstring jobjson,
    jstring taskjson,
    jstring tddjson,
    jobject taskStateManagerWrapper,
    jobject omniTaskWrapper,
    jobject taskOperatorEventGateway)
try {
    const char* jobString = jniEnv->GetStringUTFChars(jobjson, nullptr);
    std::string jobInfoString(jobString);
    jniEnv->ReleaseStringUTFChars(jobjson, jobString);

    const char* taskString = jniEnv->GetStringUTFChars(taskjson, nullptr);
    std::string taskInfoString(taskString);
    jniEnv->ReleaseStringUTFChars(taskjson, taskString);

    const char* tddString = jniEnv->GetStringUTFChars(tddjson, nullptr);
    std::string taskDDString(tddString);
    jniEnv->ReleaseStringUTFChars(tddjson, tddString);

    LOG("jobjson " << jobInfoString);
    LOG("taskjson " << taskInfoString);
    LOG("tddjson " << taskDDString);

    nlohmann::json job = nlohmann::json::parse(jobInfoString);
    omnistream::JobInformationPOD jobInfo = job;
    nlohmann::json task = nlohmann::json::parse(taskInfoString);
    omnistream::TaskInformationPOD taskInfo = task;
    nlohmann::json tdd = nlohmann::json::parse(taskDDString);
    omnistream::TaskDeploymentDescriptorPOD tddinfo = tdd;
    taskInfo.validateInputGateCount(tddinfo.getInputGates().size());
    jobject globalTaskStateRef = jniEnv->NewGlobalRef(taskStateManagerWrapper);
    if (globalTaskStateRef == nullptr) {
        LOG_TRACE("Failed to create global reference for TaskState!");
    }
    LOG_TRACE("C++ Task object created. Global ref acquired.");
    std::shared_ptr<TaskStateManagerBridge> stateBridge =
        std::make_shared<TaskStateManagerBridgeImpl>(globalTaskStateRef);

    // create the global referecen for the omnitaskwrapper
    jobject globalOmniTaskRef = jniEnv->NewGlobalRef(omniTaskWrapper);
    if (globalOmniTaskRef == nullptr) {
        LOG_TRACE("Failed to create global reference for globalOmniTaskRef!");
    }
    LOG_TRACE("C++ Task object created. Global ref acquired.");
    std::shared_ptr<OmniTaskBridge> TaskBridge = std::make_shared<OmniTaskBridgeImpl2>(globalOmniTaskRef);
    // create the global reference for the taskOperatorEventGateway
    jobject globaltaskOperatorEventGatewayref = jniEnv->NewGlobalRef(taskOperatorEventGateway);
    if (globaltaskOperatorEventGatewayref == nullptr) {
        LOG_TRACE("Failed to create global reference for globaltaskOperatorEventGatewayref!");
    }
    LOG_TRACE("C++ Task object created. Global ref acquired.");
    std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge =
        std::make_shared<TaskOperatorEventGatewayBridgeImpl>(globaltaskOperatorEventGatewayref);
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge = std::make_shared<RemoteDataFetcherBridgeImpl>();
    auto omniTaskExecutor = reinterpret_cast<omnistream::OmniTaskExecutor*>(nativeTaskExecutorAddress);
    auto nativeTask = omniTaskExecutor->submitTask(
        jobInfo, taskInfo, tddinfo, stateBridge, TaskBridge, TaskOperatorEventGatewayBridge, remoteDataFetcherBridge);
    return reinterpret_cast<jlong>(nativeTask);
} catch (const std::exception& error) {
    if (!jniEnv->ExceptionCheck()) {
        jclass exceptionClass = jniEnv->FindClass("java/lang/IllegalArgumentException");
        if (exceptionClass != nullptr) {
            jniEnv->ThrowNew(exceptionClass, error.what());
            jniEnv->DeleteLocalRef(exceptionClass);
        }
    }
    return -1;
}
