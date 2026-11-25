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

#include "OmniLocalInputChannelBridgeImpl.h"

OmniLocalInputChannelBridgeImpl::~OmniLocalInputChannelBridgeImpl()
{
    ClearJavaChannelRef();
}

void OmniLocalInputChannelBridgeImpl::RegisterJavaOmniLocalInputChannel(JNIEnv* env, jobject javaObject)
{
    if (javaOmniLocalInputChannel) {
        env->DeleteGlobalRef(javaOmniLocalInputChannel);
        javaOmniLocalInputChannel = nullptr;
    }
    if (javaObject) {
        javaOmniLocalInputChannel = env->NewGlobalRef(javaObject);
    }
}

void OmniLocalInputChannelBridgeImpl::ClearJavaChannelRef()
{
    if (!javaOmniLocalInputChannel) {
        return;
    }
    if (!g_OmniStreamJVM) {
        javaOmniLocalInputChannel = nullptr;
        return;
    }

    JNIEnv* env = nullptr;
    bool attached = false;
    if (g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != 0) {
            return;
        }
        attached = true;
    }

    env->DeleteGlobalRef(javaOmniLocalInputChannel);
    javaOmniLocalInputChannel = nullptr;

    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}

void OmniLocalInputChannelBridgeImpl::InvokeDoResumeConsumption()
{
    if (!g_OmniStreamJVM || !javaOmniLocalInputChannel) {
        return;
    }

    JNIEnv* env = nullptr;
    bool attached = false;
    if (g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != 0) {
            return;
        }
        attached = true;
    }

    jclass cls = env->GetObjectClass(javaOmniLocalInputChannel);
    if (!cls) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }

    jmethodID mid = env->GetMethodID(cls, "doResumeConsumption", "()V");
    env->DeleteLocalRef(cls);
    if (!mid) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }

    env->CallVoidMethod(javaOmniLocalInputChannel, mid);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }

    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}
