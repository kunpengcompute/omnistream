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

#ifndef TASKSTATEMANAGERBRIDGEIMPL_H
#define TASKSTATEMANAGERBRIDGEIMPL_H

#include <fstream>
#include <jni.h>
#include <state/bridge/TaskStateManagerBridge.h>
#include <common/global.h>

#include "runtime/state/SnapshotResult.h"
#include <iostream>
#include "checkpoint/TaskStateSnapshotDeserializer.h"

namespace omnistream {

class TaskStateManagerBridgeImpl : public TaskStateManagerBridge {
public:
    explicit TaskStateManagerBridgeImpl(jobject mGlobalTaskStateMgrRef)
    {
        this->m_globalTaskStateMgrRef = mGlobalTaskStateMgrRef;
    }
    ~TaskStateManagerBridgeImpl() override
    {
        if (m_globalTaskStateMgrRef == nullptr || g_OmniStreamJVM == nullptr) {
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
        env->DeleteGlobalRef(m_globalTaskStateMgrRef);
        m_globalTaskStateMgrRef = nullptr;
        if (attachedHere) {
            g_OmniStreamJVM->DetachCurrentThread();
        }
    }
    void ReportTaskStateSnapshots(
        std::string& checkpointMetaDataJson,
        std::string& checkpointMetricsJson,
        std::string& acknowledgedStateJson,
        std::string& localStateJson) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID reportMethodId = env->GetMethodID(
                taskStateManagerWrapperClass,
                "reportTaskStateSnapshots",
                "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
            if (reportMethodId == nullptr) {
                GErrorLog("Error: Could not find method reportTaskStateSnapshots.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointMetaData = env->NewStringUTF(checkpointMetaDataJson.c_str());
            jstring checkpointMetrics = env->NewStringUTF(checkpointMetricsJson.c_str());
            jstring acknowledgedState = env->NewStringUTF(acknowledgedStateJson.c_str());
            jstring localState = env->NewStringUTF(localStateJson.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(
                m_globalTaskStateMgrRef,
                reportMethodId,
                checkpointMetaData,
                checkpointMetrics,
                acknowledgedState,
                localState);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointMetaData);
            env->DeleteLocalRef(checkpointMetrics);
            env->DeleteLocalRef(acknowledgedState);
            env->DeleteLocalRef(localState);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    };
    void notifyCheckpointAborted(std::string checkpointId) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID notifyCheckpointAbortedMethodId =
                env->GetMethodID(taskStateManagerWrapperClass, "notifyCheckpointAborted", "(Ljava/lang/String;)V");
            if (notifyCheckpointAbortedMethodId == nullptr) {
                GErrorLog("Error: Could not find method notifyCheckpointAborted.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointIdstr = env->NewStringUTF(checkpointId.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskStateMgrRef, notifyCheckpointAbortedMethodId, checkpointIdstr);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointIdstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    }

    void NotifyCheckpointComplete(std::string checkpointId) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID notifyCheckpointCompleteMethodId =
                env->GetMethodID(taskStateManagerWrapperClass, "notifyCheckpointComplete", "(Ljava/lang/String;)V");
            if (notifyCheckpointCompleteMethodId == nullptr) {
                GErrorLog("Error: Could not find method notifyCheckpointAborted.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointIdstr = env->NewStringUTF(checkpointId.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskStateMgrRef, notifyCheckpointCompleteMethodId, checkpointIdstr);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointIdstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }
        g_OmniStreamJVM->DetachCurrentThread();
    }

    std::shared_ptr<TaskStateSnapshot> RetrieveLocalState(long restoreCheckpointId)
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside RetrieveLocalState");
            return nullptr;
        }

        std::shared_ptr<TaskStateSnapshot> taskStateSnapshot = nullptr;

        try {
            if (m_globalTaskStateMgrRef != nullptr) {
                jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
                if (taskStateManagerWrapperClass == nullptr) {
                    GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                    g_OmniStreamJVM->DetachCurrentThread();
                    return nullptr;
                }

                jmethodID retrieveMethodId =
                    env->GetMethodID(taskStateManagerWrapperClass, "retrieveLocalState", "(J)Ljava/lang/String;");
                if (retrieveMethodId == nullptr) {
                    GErrorLog("Error: Could not find method retrieveLocalState.");
                    env->DeleteLocalRef(taskStateManagerWrapperClass);
                    g_OmniStreamJVM->DetachCurrentThread();
                    return nullptr;
                }

                // 调用Java方法
                jstring ret = (jstring)env->CallObjectMethod(
                    m_globalTaskStateMgrRef, retrieveMethodId, (jlong)restoreCheckpointId);

                // 检查异常
                if (env->ExceptionCheck()) {
                    GErrorLog("Error: Exception occurred during Java method invocation.");
                    env->ExceptionDescribe();
                    env->ExceptionClear();
                    env->DeleteLocalRef(taskStateManagerWrapperClass);
                    g_OmniStreamJVM->DetachCurrentThread();
                    return nullptr;
                }

                // 处理返回结果
                if (ret != nullptr) {
                    const char* resultStr = env->GetStringUTFChars(ret, nullptr);
                    if (resultStr == nullptr) {
                        GErrorLog("Error: resultStr is null");
                        env->ExceptionDescribe();
                        env->ExceptionClear();
                        env->DeleteLocalRef(taskStateManagerWrapperClass);
                        g_OmniStreamJVM->DetachCurrentThread();
                        return nullptr;
                    }
                    std::string snapshotInfoString(resultStr);
                    env->ReleaseStringUTFChars(ret, resultStr);

                    // 判断结果是否为空
                    if (snapshotInfoString == "NULL") {
                        // No local state snapshot is expected for a fresh start or unavailable local recovery.
                    } else if (snapshotInfoString == "ERROR") {
                        GErrorLog("Java side returned ERROR - exception occurred");
                    } else if (!snapshotInfoString.empty()) {
                        // 非空结果，进行JSON解析和类转换
                        try {
                            nlohmann::json snapshotJson = nlohmann::json::parse(snapshotInfoString);

                            // 在这里添加从JSON到TaskStateSnapshot的转换逻辑
                            // 例如：taskStateSnapshot = ConvertJsonToTaskStateSnapshot(snapshotJson);
                            // 暂时返回一个空的shared_ptr，你需要实现具体的转换逻辑
                            taskStateSnapshot = TaskStateSnapshotDeserializer::Deserialize(snapshotJson.dump());
                        } catch (const std::exception& e) {
                            std::stringstream errorMsg;
                            errorMsg << "Failed to parse JSON: " << e.what();
                            GErrorLog(errorMsg.str());
                        }
                    } else {
                        GErrorLog("Received empty string from Java side");
                    }
                } else {
                    GErrorLog("Java method returned null string");
                }

                // 清理本地引用
                env->DeleteLocalRef(taskStateManagerWrapperClass);
                if (ret != nullptr) {
                    env->DeleteLocalRef(ret);
                }
            } else {
                GErrorLog("Error: m_globalTaskStateMgrRef is null");
            }
        } catch (const std::exception& e) {
            std::stringstream errorMsg;
            errorMsg << "Exception in RetrieveLocalState: " << e.what();
            GErrorLog(errorMsg.str());
        }

        g_OmniStreamJVM->DetachCurrentThread();
        return taskStateSnapshot;
    }

private:
    jobject m_globalTaskStateMgrRef;
};

} // namespace omnistream

#endif // TASKSTATEMANAGERBRIDGEIMPL_H
