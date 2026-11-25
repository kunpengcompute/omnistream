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

#include "RemoteDataFetcherBridgeImpl.h"

RemoteDataFetcherBridgeImpl::~RemoteDataFetcherBridgeImpl()
{
    // This destructor is what releases the JNI global ref on the Java RemoteDataFetcher. While it
    // does not run, that object pins its remote input channels, their SingleInputGate, and through
    // the gate's partitionProducerStateProvider the whole Java OmniTask and its TaskMetricGroup.
    CleanJavaRemoteDataFetcher();
}

void RemoteDataFetcherBridgeImpl::SetJavaRemoteDataFetcher(JNIEnv* env, jobject fetcher)
{
    if (javaRemoteDataFetcherRef_) {
        env->DeleteGlobalRef(javaRemoteDataFetcherRef_);
        javaRemoteDataFetcherRef_ = nullptr;
    }
    if (fetcher) {
        javaRemoteDataFetcherRef_ = env->NewGlobalRef(fetcher);
    }
}

void RemoteDataFetcherBridgeImpl::InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex)
{
    if (!g_OmniStreamJVM || !javaRemoteDataFetcherRef_) {
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

    jclass cls = env->GetObjectClass(javaRemoteDataFetcherRef_);
    if (!cls) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }
    jmethodID mid = env->GetMethodID(cls, "doResumeConsumption", "(II)V");

    env->DeleteLocalRef(cls);
    if (!mid) {
        if (attached) g_OmniStreamJVM->DetachCurrentThread();
        return;
    }
    env->CallVoidMethod(javaRemoteDataFetcherRef_, mid, (jint)inputGateIndex, (jint)channelIndex);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }

    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}

void RemoteDataFetcherBridgeImpl::ReleaseJavaRemoteDataFetcher()
{
    CleanJavaRemoteDataFetcher();
}

void RemoteDataFetcherBridgeImpl::CleanJavaRemoteDataFetcher()
{
    if (!g_OmniStreamJVM || !javaRemoteDataFetcherRef_) {
        return;
    }
    JNIEnv* env = nullptr;
    bool attached = false;
    if (g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        if (g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) == 0) {
            attached = true;
        } else {
            return;
        }
    }
    if (javaRemoteDataFetcherRef_) {
        env->DeleteGlobalRef(javaRemoteDataFetcherRef_);
        javaRemoteDataFetcherRef_ = nullptr;
    }
    if (attached) {
        g_OmniStreamJVM->DetachCurrentThread();
    }
}
