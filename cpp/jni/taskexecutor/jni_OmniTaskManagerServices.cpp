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

#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>
#include <nlohmann/json.hpp>

#include "com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices.h"
#include "runtime/taskexecutor/TaskManagerServices.h"
#include "common.h"

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices
 * Method:    createOmniTaskManagerServices
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices_createOmniTaskManagerServices(
    JNIEnv* jniEnv, jclass thiz, jstring taskManageServicesConfiguration)
{
    const char* taskMSCString = jniEnv->GetStringUTFChars(taskManageServicesConfiguration, nullptr);
    std::string taskMSConfString(taskMSCString);
    jniEnv->ReleaseStringUTFChars(taskManageServicesConfiguration, taskMSCString);

    LOG("taskMSConfString " << taskMSConfString);

    nlohmann::json taskMCS = nlohmann::json::parse(taskMSConfString);
    omnistream::TaskManagerServiceConfigurationPOD taskMCSConfPOD = taskMCS;

    LOG("taskMCSConfPOD " << taskMCSConfPOD.toString());
    auto taskManagerServices = omnistream::TaskManagerServices::fromConfiguration(taskMCSConfPOD);

    return reinterpret_cast<jlong>(taskManagerServices);
}
