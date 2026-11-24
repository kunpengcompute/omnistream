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

#ifndef TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H
#define TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H

#include <common.h>
#include <jni.h>
#include <common/global.h>
#include <state/bridge/TaskOperatorEventGatewayBridge.h>

#include "state/bridge/OmniTaskBridge.h"

class TaskOperatorEventGatewayBridgeImpl : public omnistream::TaskOperatorEventGatewayBridge {
public:
    explicit TaskOperatorEventGatewayBridgeImpl(jobject m_globalTaskOperatorEventGateWayRef)
    {
        this->m_globalTaskOperatorEventGateWayRef = m_globalTaskOperatorEventGateWayRef;
    }
    ~TaskOperatorEventGatewayBridgeImpl() override
    {
        if (m_globalTaskOperatorEventGateWayRef == nullptr || g_OmniStreamJVM == nullptr) {
            return;
        }
        JNIEnv* env = nullptr;
        bool attachedHere = false;
        jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
        if (ret == JNI_EDETACHED) {
            if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != JNI_OK) {
                return;
            }
            attachedHere = true;
        } else if (ret != JNI_OK || env == nullptr) {
            return;
        }
        env->DeleteGlobalRef(m_globalTaskOperatorEventGateWayRef);
        m_globalTaskOperatorEventGateWayRef = nullptr;
        if (attachedHere) {
            g_OmniStreamJVM->DetachCurrentThread();
        }
    }
    void sendOperatorEventToCoordinator(std::string operatorid, std::string event) override
    {
        JNIEnv* env;
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            return;
        }

        if (m_globalTaskOperatorEventGateWayRef != nullptr) {
            jclass TaskOperatorEventGateWayClass = env->GetObjectClass(m_globalTaskOperatorEventGateWayRef);
            if (TaskOperatorEventGateWayClass == nullptr) {
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jmethodID sendOperatorEventToCoordinatorMethodId = env->GetMethodID(
                TaskOperatorEventGateWayClass,
                "sendOperatorEventToCoordinator",
                "(Ljava/lang/String;Ljava/lang/String;)V");
            if (sendOperatorEventToCoordinatorMethodId == nullptr) {
                env->DeleteLocalRef(TaskOperatorEventGateWayClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring operatoridstr = env->NewStringUTF(operatorid.c_str());
            jstring eventstr = env->NewStringUTF(event.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(
                m_globalTaskOperatorEventGateWayRef, sendOperatorEventToCoordinatorMethodId, operatoridstr, eventstr);

            if (env->ExceptionCheck()) {
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            env->DeleteLocalRef(TaskOperatorEventGateWayClass);
            env->DeleteLocalRef(eventstr);
            env->DeleteLocalRef(operatoridstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    }

public:
    jobject m_globalTaskOperatorEventGateWayRef;
};

#endif // TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H
