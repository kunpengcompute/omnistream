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
#include <memory>
#include "common.h"
#include "org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"

namespace {
// A copied callback keeps the Java reference alive until an in-flight notification returns.
class JavaReaderNotification {
public:
    JavaReaderNotification(JNIEnv* env, jobject reader)
    {
        env->GetJavaVM(&jvm_);
        jclass cls = env->GetObjectClass(reader);
        method_ = env->GetMethodID(cls, "notifyNativeDataAvailable", "()V");
        env->DeleteLocalRef(cls);
        if (method_) {
            reader_ = env->NewGlobalRef(reader);
        }
    }

    ~JavaReaderNotification()
    {
        bool attached = false;
        JNIEnv* env = GetEnv(attached);
        if (env && reader_) {
            env->DeleteGlobalRef(reader_);
        }
        if (attached) {
            jvm_->DetachCurrentThread();
        }
    }

    bool IsValid() const
    {
        return reader_ && method_;
    }

    void Notify()
    {
        bool attached = false;
        JNIEnv* env = GetEnv(attached);
        if (env) {
            env->CallVoidMethod(reader_, method_);
            if (env->ExceptionCheck()) {
                env->ExceptionDescribe();
                env->ExceptionClear();
                ERROR_RELEASE("Failed to notify Java network reader");
            }
        }
        if (attached) {
            jvm_->DetachCurrentThread();
        }
    }

private:
    JNIEnv* GetEnv(bool& attached)
    {
        JNIEnv* env = nullptr;
        const jint status = jvm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
        if (status == JNI_EDETACHED) {
            if (jvm_->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != JNI_OK) {
                return nullptr;
            }
            attached = true;
        } else if (status != JNI_OK) {
            return nullptr;
        }
        return env;
    }

    JavaVM* jvm_ = nullptr;
    jobject reader_ = nullptr;
    jmethodID method_ = nullptr;
};
} // namespace

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_registerDataAvailableListener(
    JNIEnv* env, jobject reader, jlong nativeReaderRef)
{
    auto* nativeReader = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(nativeReaderRef);
    auto notification = std::make_shared<JavaReaderNotification>(env, reader);
    if (notification->IsValid()) {
        nativeReader->SetDataAvailableCallback([notification] { notification->Notify(); });
    }
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getNextBufferType(
    JNIEnv*, jobject, jlong nativeReaderRef)
{
    return reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(nativeReaderRef)
        ->GetNextBufferType();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_stopNativeReader(
    JNIEnv*, jobject, jlong nativeReaderRef)
{
    reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(nativeReaderRef)->Stop();
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_createNativeCreditBasedSequenceNumberingViewReader(
    JNIEnv* jniEnv,
    jobject input,
    jlong nativeTask,
    jlong resultBufferAddress,
    jstring partitionIdJson,
    jint subPartitionId,
    jboolean downStreamIsNative)
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
    // Notifies the producing partition that this consumer is done, which may delete the upstream task.
    // The view reader is not owned by that task, so it is safe to free it afterwards.
    viewReader->SetDataAvailableCallback(nullptr);
    viewReader->releaseAllResources();
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
    try {
        task->notifyDataAvailable();
    } catch (const std::exception& error) {
        jclass exceptionClass = jniEnv->FindClass("java/lang/IllegalStateException");
        if (exceptionClass) {
            jniEnv->ThrowNew(exceptionClass, error.what());
            jniEnv->DeleteLocalRef(exceptionClass);
        }
    } catch (...) {
        jclass exceptionClass = jniEnv->FindClass("java/lang/IllegalStateException");
        if (exceptionClass) {
            jniEnv->ThrowNew(exceptionClass, "Native output drain failed");
            jniEnv->DeleteLocalRef(exceptionClass);
        }
    }
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
