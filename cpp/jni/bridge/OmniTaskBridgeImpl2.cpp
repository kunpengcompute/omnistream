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

#include "OmniTaskBridgeImpl2.h"
#include "state/filesystem/FileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "typeinfo/TypeInfoFactory.h"
#include <limits>

enum class StreamStateHandleType {
    Unknown,
    ByteStreamStateHandle,
    RelativeFileStateHandle,
    FileStateHandle
};

void OmniTaskBridgeImpl2::declineCheckpoint(
    std::string& checkpointIDJson, std::string& failure_reasonJson, std::string& exceptionJson)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return;
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jmethodID declinedMethodId = env->GetMethodID(
            omniTaskWrapperClass, "declineCheckpoint", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
        if (declinedMethodId == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jstring checkpointid = env->NewStringUTF(checkpointIDJson.c_str());
        jstring failurereason = env->NewStringUTF(failure_reasonJson.c_str());
        jstring exceptionStr = env->NewStringUTF(exceptionJson.c_str());

        // 3. Invoke the Java method
        env->CallVoidMethod(m_globalOmniTaskRef, declinedMethodId, checkpointid, failurereason, exceptionStr);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
        }

        env->DeleteLocalRef(omniTaskWrapperClass);
        env->DeleteLocalRef(checkpointid);
        env->DeleteLocalRef(failurereason);
        env->DeleteLocalRef(exceptionStr);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

OmniTaskBridgeImpl2::~OmniTaskBridgeImpl2()
{
    // This destructor is what releases the JNI global ref on the Java OmniTask. While it does not
    // run, that Java object -- and the TaskMetricGroup it holds -- stays alive for the life of the
    // TaskManager, which is a heap leak of roughly 32 groups per job submission.
    if (m_globalOmniTaskRef != nullptr) {
        JNIEnv* env;
        // Attach the current thread to the Jvm
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            return;
        }
        env->DeleteGlobalRef(m_globalOmniTaskRef);
        m_globalOmniTaskRef = nullptr; // Clear the reference
        g_OmniStreamJVM->DetachCurrentThread();
    }
}

std::string JstringToString(JNIEnv* env, jstring jstr)
{
    if (!env || !jstr) {
        return "";
    }

    const char* cStr = env->GetStringUTFChars(jstr, nullptr);
    if (!cStr) {
        env->ExceptionClear();
        return "";
    }

    std::string result(cStr);
    env->ReleaseStringUTFChars(jstr, cStr);
    return result;
}

std::string FlinkPathToString(JNIEnv* env, jobject flinkPathObj)
{
    if (!env || !flinkPathObj) {
        return "";
    }

    const char* pathClassPath = "org/apache/flink/core/fs/Path";
    jclass pathClass = env->FindClass(pathClassPath);
    if (!pathClass) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find org.apache.flink.core.fs.Path class");
    }

    jmethodID getPathMethod = env->GetMethodID(pathClass, "getPath", "()Ljava/lang/String;");
    if (!getPathMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to get Path.getPath() method ID");
    }

    jstring pathStr = static_cast<jstring>(env->CallObjectMethod(flinkPathObj, getPathMethod));

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to call Path.getPath() method");
    }

    std::string result = JstringToString(env, pathStr);

    env->DeleteLocalRef(pathStr);
    env->DeleteLocalRef(pathClass);

    return result;
}

std::shared_ptr<StreamStateHandle> CreateFileStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get FileStateHandle class");
    }

    jmethodID getFilePathMethod = env->GetMethodID(handleClass, "getFilePath", "()Lorg/apache/flink/core/fs/Path;");
    jmethodID getStateSizeMethod = env->GetMethodID(handleClass, "getStateSize", "()J");
    if (!getFilePathMethod || !getStateSizeMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get FileStateHandle method IDs");
    }

    jobject jFilePath = env->CallObjectMethod(handleObj, getFilePathMethod);
    jlong jStateSize = env->CallLongMethod(handleObj, getStateSizeMethod);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jFilePath) env->DeleteLocalRef(jFilePath);
        throw std::runtime_error("Failed to call FileStateHandle get methods");
    }

    std::string filePathStr = FlinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<FileStateHandle>(filePath, stateSize);
}

std::shared_ptr<StreamStateHandle> CreateRelativeFileStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get RelativeFileStateHandle class");
    }

    jmethodID getFilePathMethod = env->GetMethodID(handleClass, "getFilePath", "()Lorg/apache/flink/core/fs/Path;");
    jmethodID getRelativePathMethod = env->GetMethodID(handleClass, "getRelativePath", "()Ljava/lang/String;");
    jmethodID getStateSizeMethod = env->GetMethodID(handleClass, "getStateSize", "()J");
    if (!getFilePathMethod || !getRelativePathMethod || !getStateSizeMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get RelativeFileStateHandle method IDs");
    }

    jobject jFilePath = env->CallObjectMethod(handleObj, getFilePathMethod);
    jobject jRelativePath = env->CallObjectMethod(handleObj, getRelativePathMethod);
    jlong jStateSize = env->CallLongMethod(handleObj, getStateSizeMethod);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jFilePath) env->DeleteLocalRef(jFilePath);
        if (jRelativePath) env->DeleteLocalRef(jRelativePath);
        throw std::runtime_error("Failed to call RelativeFileStateHandle get methods");
    }

    std::string filePathStr = FlinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    std::string relativePath = JstringToString(env, static_cast<jstring>(jRelativePath));
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(jRelativePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<RelativeFileStateHandle>(filePath, relativePath, stateSize);
}

std::shared_ptr<StreamStateHandle> CreateByteStreamStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get ByteStreamStateHandle class");
    }

    jmethodID getHandleNameMethod = env->GetMethodID(handleClass, "getHandleName", "()Ljava/lang/String;");
    jmethodID getDataMethod = env->GetMethodID(handleClass, "getData", "()[B");
    if (!getHandleNameMethod || !getDataMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get ByteStreamStateHandle method IDs");
    }

    jobject jHandleName = env->CallObjectMethod(handleObj, getHandleNameMethod);
    jbyteArray jData = static_cast<jbyteArray>(env->CallObjectMethod(handleObj, getDataMethod));

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jHandleName) env->DeleteLocalRef(jHandleName);
        if (jData) env->DeleteLocalRef(jData);
        throw std::runtime_error("Failed to call ByteStreamStateHandle get methods");
    }

    std::string handleName = JstringToString(env, static_cast<jstring>(jHandleName));
    std::vector<uint8_t> data;

    if (jData) {
        jsize dataLen = env->GetArrayLength(jData);
        jbyte* dataBytes = env->GetByteArrayElements(jData, nullptr);

        if (dataBytes) {
            data.assign(reinterpret_cast<uint8_t*>(dataBytes), reinterpret_cast<uint8_t*>(dataBytes + dataLen));
            env->ReleaseByteArrayElements(jData, dataBytes, 0);
        }
        env->DeleteLocalRef(jData);
    }

    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<ByteStreamStateHandle>(handleName, data);
}

StreamStateHandleType GetHandleType(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        return StreamStateHandleType::Unknown;
    }

    const char* byteStreamClassPath = "org/apache/flink/runtime/state/memory/ByteStreamStateHandle";
    const char* relativeFileClassPath = "org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle";
    const char* fileClassPath = "org/apache/flink/runtime/state/filesystem/FileStateHandle";

    jclass byteStreamClass = env->FindClass(byteStreamClassPath);
    jclass relativeFileClass = env->FindClass(relativeFileClassPath);
    jclass fileClass = env->FindClass(fileClassPath);

    bool isByteStream = false;
    bool isRelativeFile = false;
    bool isFile = false;

    if (byteStreamClass) {
        isByteStream = env->IsInstanceOf(handleObj, byteStreamClass);
        env->DeleteLocalRef(byteStreamClass);
    }
    if (relativeFileClass) {
        isRelativeFile = env->IsInstanceOf(handleObj, relativeFileClass);
        env->DeleteLocalRef(relativeFileClass);
    }
    if (fileClass) {
        isFile = env->IsInstanceOf(handleObj, fileClass);
        env->DeleteLocalRef(fileClass);
    }

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return StreamStateHandleType::Unknown;
    }

    if (isByteStream) {
        return StreamStateHandleType::ByteStreamStateHandle;
    } else if (isRelativeFile) {
        return StreamStateHandleType::RelativeFileStateHandle;
    } else if (isFile) {
        return StreamStateHandleType::FileStateHandle;
    } else {
        return StreamStateHandleType::Unknown;
    }
}

std::shared_ptr<StreamStateHandle> GetStreamStateHandle(JNIEnv* env, jobject jHandle)
{
    std::shared_ptr<StreamStateHandle> handle;

    if (jHandle) {
        auto type = GetHandleType(env, jHandle);
        switch (type) {
            case StreamStateHandleType::FileStateHandle: handle = CreateFileStateHandle(env, jHandle); break;
            case StreamStateHandleType::RelativeFileStateHandle:
                handle = CreateRelativeFileStateHandle(env, jHandle);
                break;
            case StreamStateHandleType::ByteStreamStateHandle:
                handle = CreateByteStreamStateHandle(env, jHandle);
                break;
            default: handle = nullptr;
        }
        env->DeleteLocalRef(jHandle);
    }
    return handle;
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> ConvertSnapshotResult(JNIEnv* env, jobject jSnapshotResult)
{
    jclass snapshotResultClass = env->FindClass("org/apache/flink/runtime/state/SnapshotResult");
    if (snapshotResultClass == nullptr) {
        return nullptr;
    }

    jmethodID midGetJobManager = env->GetMethodID(
        snapshotResultClass, "getJobManagerOwnedSnapshot", "()Lorg/apache/flink/runtime/state/StateObject;");
    if (midGetJobManager == nullptr) {
        env->DeleteLocalRef(snapshotResultClass);
        return nullptr;
    }

    jmethodID midGetTaskLocal =
        env->GetMethodID(snapshotResultClass, "getTaskLocalSnapshot", "()Lorg/apache/flink/runtime/state/StateObject;");
    if (midGetTaskLocal == nullptr) {
        env->DeleteLocalRef(snapshotResultClass);
        return nullptr;
    }

    jobject jobManagerSnapshotJobj = env->CallObjectMethod(jSnapshotResult, midGetJobManager);
    jobject taskLocalSnapshotJobj = env->CallObjectMethod(jSnapshotResult, midGetTaskLocal);

    auto jobManagerSnapshot = GetStreamStateHandle(env, jobManagerSnapshotJobj);
    auto taskLocalSnapshot = GetStreamStateHandle(env, taskLocalSnapshotJobj);

    env->DeleteLocalRef(snapshotResultClass);

    return SnapshotResult<StreamStateHandle>::WithLocalState(jobManagerSnapshot, taskLocalSnapshot);
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> OmniTaskBridgeImpl2::CallMaterializeMetaData(
    jlong checkpointId,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    CheckpointOptions* checkpointOptions,
    std::string keySerializer)
{
    if (m_globalOmniTaskRef == nullptr) {
        GErrorLog("StreamTask is not registered in TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }

    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }
    if (checkpointOptions == nullptr) {
        GErrorLog("checkpointOptions is nullptr in TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }
    nlohmann::json stateMetaInfoJson = nlohmann::json::array();
    for (const auto& snapshot : snapshots) {
        nlohmann::json jsonObj;
        jsonObj[StateMetaInfoSnapshot::NAME_KEY] = snapshot->getName();
        jsonObj[StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY] =
            static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj[StateMetaInfoSnapshot::OPTIONS_KEY] = snapshot->getOptionsImmutable();
        jsonObj[StateMetaInfoSnapshot::SERIALIZER_KEY] = snapshot->getSerializerJson();
        jsonObj[StateMetaInfoSnapshot::KEYED_BACKEND_KEY_SERIALIZER_KEY] = keySerializer;
        stateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string stateMetaInfoStr = stateMetaInfoJson.dump();

    std::string localRecoveryConfigStr = "{}";
    if (localRecoveryConfig != nullptr && localRecoveryConfig->IsLocalRecoveryEnabled()) {
        try {
            nlohmann::json localRecoveryJson;
            auto directoryProvider = localRecoveryConfig->GetLocalStateDirectoryProvider();

            // 序列化 allocationBaseDirs_
            nlohmann::json baseDirsArray = nlohmann::json::array();
            for (const auto& path : directoryProvider->GetPaths()) {
                baseDirsArray.push_back(path.string()); // 将filesystem::path转换为字符串
            }
            localRecoveryJson["allocationBaseDirs"] = baseDirsArray;

            // 序列化 jobID_
            localRecoveryJson["jobID"] = directoryProvider->GetJobIdHexStr();

            // 序列化 jobVertexID_
            localRecoveryJson["jobVertexID"] = directoryProvider->GetVertexIdHexStr();

            // 序列化 subtaskIndex_
            localRecoveryJson["subtaskIndex"] = directoryProvider->GetSubIndex();

            localRecoveryConfigStr = localRecoveryJson.dump();

        } catch (const std::exception& e) {
            std::stringstream errorMsg;
            errorMsg << "Failed to serialize localRecoveryConfig: " << e.what();
            GErrorLog(errorMsg.str());
            localRecoveryConfigStr = "{}"; // 序列化失败时使用空JSON
        }
    }
    nlohmann::json jcheckpointOptions = checkpointOptions->ToJson();
    std::string checkpointOptionsStr = jcheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "materializeMetaData",
        "(JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lorg/apache/flink/runtime/state/SnapshotResult;");
    jstring jStateMetaInfoStr = env->NewStringUTF(stateMetaInfoStr.c_str());
    jstring jLocalRecoveryConfigStr = env->NewStringUTF(localRecoveryConfigStr.c_str());
    jstring jcheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    jobject resultObj = env->CallObjectMethod(
        m_globalOmniTaskRef, mid, checkpointId, jStateMetaInfoStr, jLocalRecoveryConfigStr, jcheckpointOptionsStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jStateMetaInfoStr);
        env->DeleteLocalRef(jLocalRecoveryConfigStr);
        env->DeleteLocalRef(cls);
        env->DeleteLocalRef(jcheckpointOptionsStr);
        throw std::runtime_error("Failed to call materializeMetaData");
    }
    env->DeleteLocalRef(jStateMetaInfoStr);
    env->DeleteLocalRef(jLocalRecoveryConfigStr);
    env->DeleteLocalRef(cls);
    env->DeleteLocalRef(jcheckpointOptionsStr);

    try {
        auto result = ConvertSnapshotResult(env, resultObj);
        if (resultObj != nullptr) {
            env->DeleteLocalRef(resultObj);
        }
        return result;
    } catch (...) {
        if (resultObj != nullptr) {
            env->DeleteLocalRef(resultObj);
        }
        throw;
    }
}

jobject OmniTaskBridgeImpl2::CallUploadFilesToCheckpointFs(
    const std::vector<Path>& filePaths, int numberOfSnapshottingThreads)
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallUploadFilesToCheckpointFs");
        return nullptr;
    }

    nlohmann::json jPaths = nlohmann::json::array();
    for (const auto& p : filePaths) {
        jPaths.push_back(p.toString());
    }
    std::string pathsJsonStr = jPaths.dump();

    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "uploadFilesToCheckpointFs", "(Ljava/lang/String;I)Ljava/util/List;");
    jstring jPathsJson = env->NewStringUTF(pathsJsonStr.c_str());
    jobject jResult = env->CallObjectMethod(m_globalOmniTaskRef, mid, jPathsJson, numberOfSnapshottingThreads);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jPathsJson);
        env->DeleteLocalRef(cls);
        throw std::runtime_error("Failed to call uploadFilesToCheckpointFs");
    }

    env->DeleteLocalRef(jPathsJson);
    env->DeleteLocalRef(cls);

    return jResult;
}

jobject ConvertToJavaByteStreamStateHandle(JNIEnv* env, const ByteStreamStateHandle& cppHandle)
{
    // 1. 获取 Java 类和方法 ID
    jclass byteStreamStateHandleClass = env->FindClass("org/apache/flink/runtime/state/memory/ByteStreamStateHandle");
    if (!byteStreamStateHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(byteStreamStateHandleClass, "<init>", "(Ljava/lang/String;[B)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }

    // 2. 转换 handleName
    jstring jHandleName = env->NewStringUTF(cppHandle.GetHandleName().c_str());
    if (!jHandleName) {
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }

    // 3. 转换 data (std::vector<uint8_t> -> jbyteArray)
    const auto& cppData = cppHandle.GetData();
    jbyteArray jData = env->NewByteArray(static_cast<jsize>(cppData.size()));
    if (!jData) {
        env->DeleteLocalRef(jHandleName);
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }
    env->SetByteArrayRegion(
        jData, 0, static_cast<jsize>(cppData.size()), reinterpret_cast<const jbyte*>(cppData.data()));

    // 4. 创建 Java 对象
    jobject javaHandle = env->NewObject(byteStreamStateHandleClass, constructor, jHandleName, jData);

    // 5. 清理局部引用
    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(jData);
    env->DeleteLocalRef(byteStreamStateHandleClass);

    return javaHandle;
}

jobject ConvertToJavaFileStateHandle(JNIEnv* env, const FileStateHandle& cppHandle)
{
    // 1. 获取Java类和方法
    jclass fileStateHandleClass = env->FindClass("org/apache/flink/runtime/state/filesystem/FileStateHandle");
    if (!fileStateHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(fileStateHandleClass, "<init>", "(Lorg/apache/flink/core/fs/Path;J)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(fileStateHandleClass);
        return nullptr;
    }

    // 2. 转换文件路径（Path对象）
    // 先获取Java的Path类
    jclass pathClass = env->FindClass("org/apache/flink/core/fs/Path");
    jmethodID pathConstructor = env->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");

    // 将C++路径转换为Java字符串
    std::string filePathStr = cppHandle.GetFilePath().toString();
    jstring jFilePathStr = env->NewStringUTF(filePathStr.c_str());

    // 创建Java Path对象
    jobject javaPath = env->NewObject(pathClass, pathConstructor, jFilePathStr);

    // 3. 创建Java FileStateHandle对象
    jobject javaHandle =
        env->NewObject(fileStateHandleClass, constructor, javaPath, static_cast<jlong>(cppHandle.GetStateSize()));

    // 4. 清理局部引用
    env->DeleteLocalRef(jFilePathStr);
    env->DeleteLocalRef(javaPath);
    env->DeleteLocalRef(pathClass);
    env->DeleteLocalRef(fileStateHandleClass);

    return javaHandle;
}

jobject ConvertToJavaRelativeFileStateHandle(JNIEnv* env, const RelativeFileStateHandle& cppHandle)
{
    // 1. 获取Java类和方法
    jclass relativeHandleClass = env->FindClass("org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle");
    if (!relativeHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor =
        env->GetMethodID(relativeHandleClass, "<init>", "(Lorg/apache/flink/core/fs/Path;Ljava/lang/String;J)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(relativeHandleClass);
        return nullptr;
    }

    // 2. 转换文件路径（复用FileStateHandle的Path转换逻辑）
    jclass pathClass = env->FindClass("org/apache/flink/core/fs/Path");
    jmethodID pathConstructor = env->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");

    std::string filePathStr = cppHandle.GetFilePath().toString();
    jstring jFilePathStr = env->NewStringUTF(filePathStr.c_str());
    jobject javaPath = env->NewObject(pathClass, pathConstructor, jFilePathStr);

    // 3. 转换相对路径
    jstring jRelativePath = env->NewStringUTF(cppHandle.GetRelativePath().c_str());

    // 4. 创建Java对象
    jobject javaHandle = env->NewObject(
        relativeHandleClass, constructor, javaPath, jRelativePath, static_cast<jlong>(cppHandle.GetStateSize()));

    // 5. 清理局部引用
    env->DeleteLocalRef(jFilePathStr);
    env->DeleteLocalRef(javaPath);
    env->DeleteLocalRef(jRelativePath);
    env->DeleteLocalRef(pathClass);
    env->DeleteLocalRef(relativeHandleClass);

    return javaHandle;
}

jobject ConvertPathStrToJavaPath(JNIEnv* env, const std::filesystem::path& restoreInstancePath)
{
    // 1. 获取Java的Paths类和get方法
    jclass pathsClass = env->FindClass("java/nio/file/Paths");
    if (pathsClass == nullptr) {
        // 类未找到处理
        return nullptr;
    }

    // 2. 获取Paths.get(String, String...)方法
    jmethodID getMethod =
        env->GetStaticMethodID(pathsClass, "get", "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;");
    if (getMethod == nullptr) {
        // 方法未找到处理
        env->DeleteLocalRef(pathsClass);
        return nullptr;
    }

    // 3. 将fs::path转换为jstring
    jstring pathString = env->NewStringUTF(restoreInstancePath.string().c_str());

    // 4. 创建空的String数组作为可变参数（Paths.get的第二个参数）
    jobjectArray emptyArray = env->NewObjectArray(0, env->FindClass("java/lang/String"), nullptr);

    // 5. 调用Paths.get方法
    jobject javaPath = env->CallStaticObjectMethod(pathsClass, getMethod, pathString, emptyArray);

    // 6. 清理局部引用
    env->DeleteLocalRef(pathString);
    env->DeleteLocalRef(emptyArray);
    env->DeleteLocalRef(pathsClass);

    return javaPath;
}

jobject ConvertToJavaStreamStateHandle(JNIEnv* env, const StreamStateHandle& cppHandle)
{
    // 动态类型检查（如果是 ByteStreamStateHandle）
    if (auto byteHandle = dynamic_cast<const ByteStreamStateHandle*>(&cppHandle)) {
        return ConvertToJavaByteStreamStateHandle(env, *byteHandle);
    } else if (auto fileHandle = dynamic_cast<const FileStateHandle*>(&cppHandle)) {
        return ConvertToJavaFileStateHandle(env, *fileHandle);
    } else if (auto relHandle = dynamic_cast<const RelativeFileStateHandle*>(&cppHandle)) {
        return ConvertToJavaRelativeFileStateHandle(env, *relHandle);
    } else {
        env->ThrowNew(env->FindClass("java/lang/UnsupportedOperationException"), "Unsupported StreamStateHandle type");
        return nullptr;
    }
}

bool OmniTaskBridgeImpl2::CallDownloadFileToLocal(
    const StreamStateHandle& cppHandle, const std::string& restoreInstancePath)
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallDownloadFileToLocal");
        return false;
    }

    jobject restoreFileHandle = ConvertToJavaStreamStateHandle(env, cppHandle);
    if (restoreFileHandle == nullptr) {
        GErrorLog("Failed to convert to java stream state handle");
        return false;
    }
    jobject restoreTargetPath = ConvertPathStrToJavaPath(env, restoreInstancePath);
    if (restoreTargetPath == nullptr) {
        GErrorLog("Failed to convert to java path");
        return false;
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    // 获取CallDownloadFileToLocal方法ID
    jmethodID mid = env->GetMethodID(
        cls, "callDownloadFileToLocal", "(Lorg/apache/flink/runtime/state/StreamStateHandle;Ljava/nio/file/Path;)Z");
    auto jResult = env->CallBooleanMethod(m_globalOmniTaskRef, mid, restoreFileHandle, restoreTargetPath);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return false;
    }
    return jResult == JNI_TRUE;
}

TypeInformation* CreateTypeInfoIfValid(
    const nlohmann::json& serializerJson,
    const std::string& stateName,
    const std::string& serializerKey,
    bool tolerateUnsupportedSerializer)
{
    if (!serializerJson.is_object() || !serializerJson.contains("serializerName") ||
        !serializerJson.at("serializerName").is_string()) {
        std::string error = "serializer json is incomplete, state=" + stateName + ", serializerKey=" + serializerKey;
        if (!tolerateUnsupportedSerializer) {
            throw std::runtime_error(error);
        }
        return nullptr;
    }
    try {
        return TypeInfoFactory::createDataStreamTypeInfo(serializerJson);
    } catch (const std::exception& e) {
        if (!tolerateUnsupportedSerializer) {
            throw;
        }
        return nullptr;
    }
}

std::vector<StateMetaInfoSnapshot> convertResult(const std::string& cppResult, bool tolerateUnsupportedSerializer)
{
    // reconstruct std::vector<StateMetaInfoSnapshot>
    std::vector<StateMetaInfoSnapshot> toReturn;
    nlohmann::json parsed = nlohmann::json::parse(cppResult);
    if (!parsed.is_array()) {
        throw std::runtime_error("snapshot json result must be an array.");
    }
    for (const auto& oneSnapshot : parsed) {
        std::unordered_map<std::string, std::string> tmpOptions;
        if (!oneSnapshot.contains(StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY) ||
            !oneSnapshot.at(StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY).is_string() ||
            !oneSnapshot.contains(StateMetaInfoSnapshot::NAME_KEY) ||
            !oneSnapshot.at(StateMetaInfoSnapshot::NAME_KEY).is_string() ||
            !oneSnapshot.contains(StateMetaInfoSnapshot::OPTIONS_IMMUTABLE_KEY) ||
            !oneSnapshot.at(StateMetaInfoSnapshot::OPTIONS_IMMUTABLE_KEY).is_object()) {
            throw std::runtime_error("snapshot json format invalid.");
        }
        for (const auto& [key, value] : oneSnapshot.at(StateMetaInfoSnapshot::OPTIONS_IMMUTABLE_KEY).items()) {
            if (value.is_string()) {
                tmpOptions[key] = value.get<std::string>();
            }
        }
        std::unordered_map<std::string, TypeSerializer*> tmpSerializers;
        const std::string stateName = oneSnapshot.at(StateMetaInfoSnapshot::NAME_KEY).get<std::string>();
        if (oneSnapshot.contains(StateMetaInfoSnapshot::SERIALIZER_KEY) &&
            oneSnapshot.at(StateMetaInfoSnapshot::SERIALIZER_KEY).is_object()) {
            const auto& serializers = oneSnapshot.at(StateMetaInfoSnapshot::SERIALIZER_KEY);
            if (serializers.contains(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY) &&
                serializers.at(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY).is_object()) {
                auto namespaceSerializer = CreateTypeInfoIfValid(
                    serializers.at(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY),
                    stateName,
                    SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY,
                    tolerateUnsupportedSerializer);
                if (namespaceSerializer != nullptr) {
                    tmpSerializers.emplace(
                        StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer->getTypeSerializer());
                }
            } else if (serializers.contains(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY)) {
                std::string error =
                    std::string(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY) + " json is invalid, state=" + stateName;
                if (!tolerateUnsupportedSerializer) {
                    throw std::runtime_error(error);
                }
            }
            if (serializers.contains(SerializerJsonInfo::STATE_SERIALIZER_KEY) &&
                serializers.at(SerializerJsonInfo::STATE_SERIALIZER_KEY).is_object()) {
                auto stateSerializer = CreateTypeInfoIfValid(
                    serializers.at(SerializerJsonInfo::STATE_SERIALIZER_KEY),
                    stateName,
                    SerializerJsonInfo::STATE_SERIALIZER_KEY,
                    tolerateUnsupportedSerializer);
                if (stateSerializer != nullptr) {
                    tmpSerializers.emplace(
                        StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, stateSerializer->getTypeSerializer());
                }
            } else if (serializers.contains(SerializerJsonInfo::STATE_SERIALIZER_KEY)) {
                std::string error =
                    std::string(SerializerJsonInfo::STATE_SERIALIZER_KEY) + " json is invalid, state=" + stateName;
                if (!tolerateUnsupportedSerializer) {
                    throw std::runtime_error(error);
                }
            }
        }
        // Currently we don't take snapshot of serializers
        StateMetaInfoSnapshot::BackendStateType bst;
        auto backendStateTypeStr = oneSnapshot.at(StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY).get<std::string>();
        if (backendStateTypeStr == StateMetaInfoSnapshot::KEY_VALUE_TYPE) {
            bst = StateMetaInfoSnapshot::BackendStateType::KEY_VALUE;
        } else if (backendStateTypeStr == StateMetaInfoSnapshot::PRIORITY_QUEUE_TYPE) {
            bst = StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE;
        } else if (backendStateTypeStr == StateMetaInfoSnapshot::OPERATOR_TYPE) {
            bst = StateMetaInfoSnapshot::BackendStateType::OPERATOR;
        } else if (backendStateTypeStr == StateMetaInfoSnapshot::BROADCAST_TYPE) {
            INFO_RELEASE("Unsupport BackendStateType.");
            continue;
        } else {
            throw std::runtime_error("Unknown BackendStateType.");
        }
        toReturn.push_back(StateMetaInfoSnapshot(stateName, bst, tmpOptions, {}, tmpSerializers));
    }
    return toReturn;
}

std::vector<StateMetaInfoSnapshot> OmniTaskBridgeImpl2::readMetaData(const std::string& metaStateHandle)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return {};
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jmethodID readMetaMethodId =
            env->GetMethodID(omniTaskWrapperClass, "readMetaData", "(Ljava/lang/String;)Ljava/lang/String;");
        if (readMetaMethodId == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());

        // Invoke the Java method
        jstring result = (jstring)env->CallObjectMethod(m_globalOmniTaskRef, readMetaMethodId, msHandle);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
        }

        // Convert jstring to std::string
        const char* strChars = env->GetStringUTFChars(result, nullptr);
        std::string cppResult(strChars);
        env->ReleaseStringUTFChars(result, strChars);
        g_OmniStreamJVM->DetachCurrentThread();
        return convertResult(cppResult, false);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        return {};
    }
}

std::vector<StateMetaInfoSnapshot> OmniTaskBridgeImpl2::readOperatorMetaData(const std::string& metaStateHandle)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return {};
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jmethodID readMetaMethodId =
            env->GetMethodID(omniTaskWrapperClass, "readOperatorMetaData", "(Ljava/lang/String;)Ljava/lang/String;");
        if (readMetaMethodId == nullptr) {
            GErrorLog("Error: Could not get readOperatorMetaData method for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());

        // Invoke the Java method
        jstring result = (jstring)env->CallObjectMethod(m_globalOmniTaskRef, readMetaMethodId, msHandle);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
            INFO_RELEASE("Error: Could not call readOperatorMetaData method for JNI call");
            env->DeleteLocalRef(msHandle);
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }
        if (result == nullptr) {
            LOG("Error: readOperatorMetaData returned null");
            env->DeleteLocalRef(msHandle);
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        // Convert jstring to std::string
        const char* strChars = env->GetStringUTFChars(result, nullptr);
        if (strChars == nullptr) {
            if (env->ExceptionCheck()) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }
            LOG("Error: readOperatorMetaData failed to copy result string");
            env->DeleteLocalRef(result);
            env->DeleteLocalRef(msHandle);
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }
        std::string cppResult(strChars);
        env->ReleaseStringUTFChars(result, strChars);
        env->DeleteLocalRef(result);
        env->DeleteLocalRef(msHandle);
        env->DeleteLocalRef(omniTaskWrapperClass);
        g_OmniStreamJVM->DetachCurrentThread();
        return convertResult(cppResult, true);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        g_OmniStreamJVM->DetachCurrentThread();
        return {};
    }
}

std::vector<int8_t> jbyteArrayToVector(JNIEnv* env, jbyteArray byteArray)
{
    std::vector<int8_t> result;
    if (!byteArray) {
        return result;
    }

    jsize length = env->GetArrayLength(byteArray);
    if (length < 0) {
        return result;
    }

    result.reserve(length);
    jbyte* data = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(byteArray, nullptr));
    if (data != nullptr) {
        result.assign(data, data + length);
        env->ReleasePrimitiveArrayCritical(byteArray, data, JNI_ABORT);
    }
    return result;
}

void OmniTaskBridgeImpl2::getKeyGroupEntries(
    jobject inputStream, int& currentKvStateId, bool isUsingKeyGroupCompression, std::vector<KeyGroupEntry>& entries)
{
    entries.clear();
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Error: getKeyGroupEntries could not AttachCurrentThread for JNI call");
        return;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            GErrorLog("Error: getKeyGroupEntries could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jclass entryWrapperClass = env->FindClass("com/huawei/omniruntime/flink/runtime/restore/KeyGroupEntryWrapper");
        jclass entryClass = env->FindClass("com/huawei/omniruntime/flink/runtime/restore/KeyGroupEntry");
        if (unlikely(entryWrapperClass == nullptr || entryClass == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries could not FindClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jfieldID currentKvStateIdField = env->GetFieldID(entryWrapperClass, "currentKvStateId", "I");
        jfieldID entriesField = env->GetFieldID(
            entryWrapperClass, "entries", "[Lcom/huawei/omniruntime/flink/runtime/restore/KeyGroupEntry;");
        jfieldID entryKvStateIdField = env->GetFieldID(entryWrapperClass, "kvStateId", "I");
        jfieldID entryCountField = env->GetFieldID(entryWrapperClass, "count", "I");
        if (unlikely(
                currentKvStateIdField == nullptr || entriesField == nullptr || entryKvStateIdField == nullptr ||
                entryCountField == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries entryWrapperClass could not GetFieldID for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jfieldID entryKeyField = env->GetFieldID(entryClass, "key", "[B");
        jfieldID entryValueField = env->GetFieldID(entryClass, "value", "[B");
        if (unlikely(entryKeyField == nullptr || entryValueField == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries entryClass could not GetFieldID for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jmethodID mid = env->GetMethodID(
            omniTaskWrapperClass,
            "getKeyGroupEntries",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;IZ)Lcom/huawei/omniruntime/"
            "flink/runtime/restore/KeyGroupEntryWrapper;");

        jint jCurrentKvStateId = static_cast<jint>(currentKvStateId);
        jboolean jIsUsingKeyGroupCompression = static_cast<jboolean>(isUsingKeyGroupCompression);

        // Invoke the Java method
        jobject result = env->CallObjectMethod(
            m_globalOmniTaskRef, mid, inputStream, jCurrentKvStateId, jIsUsingKeyGroupCompression);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        if (result == nullptr) {
            GErrorLog("Error: getKeyGroupEntries get null result for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        currentKvStateId = env->GetIntField(result, currentKvStateIdField);
        int kvStateId = env->GetIntField(result, entryKvStateIdField);
        int count = env->GetIntField(result, entryCountField);

        jobjectArray entriesArray = static_cast<jobjectArray>(env->GetObjectField(result, entriesField));

        env->PushLocalFrame(16);
        for (int i = 0; i < count; i++) {
            jobject entry = env->GetObjectArrayElement(entriesArray, i);
            jbyteArray keyArray = static_cast<jbyteArray>(env->GetObjectField(entry, entryKeyField));
            jbyteArray valueArray = static_cast<jbyteArray>(env->GetObjectField(entry, entryValueField));

            entries.emplace_back(KeyGroupEntry(
                kvStateId,
                std::move(jbyteArrayToVector(env, keyArray)),
                std::move(jbyteArrayToVector(env, valueArray))));
        }
        env->PopLocalFrame(nullptr);

        env->DeleteLocalRef(entriesArray);
        env->DeleteLocalRef(entryWrapperClass);
        env->DeleteLocalRef(entryClass);
        env->DeleteLocalRef(result);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

jobject OmniTaskBridgeImpl2::getSavepointInputStream(const std::string& metaStateHandle)
{
    JNIEnv* env;
    jobject inputStream = nullptr;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: getSavepointInputStream could not AttachCurrentThread for JNI call");
        throw std::runtime_error("getSavepointInputStream could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: getSavepointInputStream could not GetObjectClass for JNI call");
            throw std::runtime_error("getSavepointInputStream could not GetObjectClass for JNI call");
        }
        jmethodID mid = env->GetMethodID(
            omniTaskWrapperClass,
            "getSavepointInputStream",
            "(Ljava/lang/String;)Lorg/apache/flink/core/fs/FSDataInputStream;");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: getSavepointInputStream could not get methodID for JNI call");
            throw std::runtime_error("getSavepointInputStream could not get methodID for JNI call");
        }
        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());
        jobject localInputStream = env->CallObjectMethod(m_globalOmniTaskRef, mid, msHandle);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteLocalRef(msHandle);
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream get method");
            throw std::runtime_error("Failed to call SavepointInputStream get method");
        }
        if (localInputStream != nullptr) {
            inputStream = env->NewGlobalRef(localInputStream);
            env->DeleteLocalRef(localInputStream);
        }
        env->DeleteLocalRef(msHandle);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        g_OmniStreamJVM->DetachCurrentThread();
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
    return inputStream;
}

int OmniTaskBridgeImpl2::ReadSavepointInputStream(jobject inputStream, int8_t* chunk, size_t offset, size_t len)
{
    if (inputStream == nullptr || chunk == nullptr || len == 0) {
        return 0;
    }

    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: ReadSavepointInputStream could not AttachCurrentThread for JNI call");
        throw std::runtime_error("ReadSavepointInputStream could not AttachCurrentThread for JNI call");
    }

    if (m_globalOmniTaskRef == nullptr) {
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: ReadSavepointInputStream could not get OmniTask wrapper");
        throw std::runtime_error("ReadSavepointInputStream could not get OmniTask wrapper");
    }

    jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
    if (omniTaskWrapperClass == nullptr) {
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: ReadSavepointInputStream could not GetObjectClass for JNI call");
        throw std::runtime_error("ReadSavepointInputStream could not GetObjectClass for JNI call");
    }

    jmethodID mid = env->GetMethodID(
        omniTaskWrapperClass, "readSavepointInputStream", "(Lorg/apache/flink/core/fs/FSDataInputStream;[BII)I");
    if (mid == nullptr) {
        env->DeleteLocalRef(omniTaskWrapperClass);
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: ReadSavepointInputStream could not GetMethodID for JNI call");
        throw std::runtime_error("ReadSavepointInputStream could not GetMethodID for JNI call");
    }

    if (len > static_cast<size_t>(std::numeric_limits<jint>::max())) {
        env->DeleteLocalRef(omniTaskWrapperClass);
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: ReadSavepointInputStream chunk too large.");
        THROW_LOGIC_EXCEPTION("ReadSavepointInputStream chunk too large: " << len);
    }

    jbyteArray byteArray = env->NewByteArray(static_cast<jsize>(len));
    if (byteArray == nullptr) {
        env->DeleteLocalRef(omniTaskWrapperClass);
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: ReadSavepointInputStream failed to allocate byte array.");
        throw std::runtime_error("ReadSavepointInputStream failed to allocate byte array");
    }

    jint read = env->CallIntMethod(m_globalOmniTaskRef, mid, inputStream, byteArray, 0, static_cast<jint>(len));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(byteArray);
        env->DeleteLocalRef(omniTaskWrapperClass);
        g_OmniStreamJVM->DetachCurrentThread();
        INFO_RELEASE("Error: Failed to call SavepointInputStream read method");
        throw std::runtime_error("Failed to call SavepointInputStream read method");
    }

    if (read > 0) {
        env->GetByteArrayRegion(byteArray, 0, read, reinterpret_cast<jbyte*>(chunk + offset));
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteLocalRef(byteArray);
            env->DeleteLocalRef(omniTaskWrapperClass);
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: ReadSavepointInputStream failed to copy byte array.");
            throw std::runtime_error("ReadSavepointInputStream failed to copy byte array");
        }
    }

    env->DeleteLocalRef(byteArray);
    env->DeleteLocalRef(omniTaskWrapperClass);
    g_OmniStreamJVM->DetachCurrentThread();
    return read;
}

bool OmniTaskBridgeImpl2::isUsingKeyGroupCompression(jobject inputStream)
{
    JNIEnv* env;
    bool result = false;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: isUsingKeyGroupCompression could not AttachCurrentThread for JNI call");
        return false;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: isUsingKeyGroupCompression could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return false;
        }
        jmethodID mid = env->GetMethodID(
            omniTaskWrapperClass, "isUsingKeyGroupCompression", "(Lorg/apache/flink/core/fs/FSDataInputStream;)Z");
        if (mid == nullptr) {
            INFO_RELEASE("Error: isUsingKeyGroupCompression could not GetMethodID for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return false;
        }
        auto ret = env->CallBooleanMethod(m_globalOmniTaskRef, mid, inputStream);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            g_OmniStreamJVM->DetachCurrentThread();
            throw std::runtime_error("Failed to call SavepointInputStream use compression method");
        }
        result = (ret == JNI_TRUE);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
    return result;
}

void OmniTaskBridgeImpl2::setSavepointInputStreamOffset(jobject inputStream, int64_t offset)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: setSavepointInputStreamOffset could not AttachCurrentThread for JNI call");
        throw std::runtime_error("setSavepointInputStreamOffset could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetObjectClass for JNI call");
            throw std::runtime_error("setSavepointInputStreamOffset could not GetObjectClass for JNI call");
        }
        jmethodID mid = env->GetMethodID(
            omniTaskWrapperClass, "setSavepointInputStreamOffset", "(Lorg/apache/flink/core/fs/FSDataInputStream;J)V");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetMethodID for JNI call");
            throw std::runtime_error("setSavepointInputStreamOffset could not GetMethodID for JNI call");
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream, offset);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream set offset method");
            throw std::runtime_error("Failed to call SavepointInputStream set offset method");
        }
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

void OmniTaskBridgeImpl2::closeSavepointInputStream(jobject inputStream)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: closeSavepointInputStream could not AttachCurrentThread for JNI call");
        throw std::runtime_error("closeSavepointInputStream could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: closeSavepointInputStream omniTaskWrapperClass == nullptr");
            throw std::runtime_error("closeSavepointInputStream omniTaskWrapperClass == nullptr");
        }
        jmethodID mid = env->GetMethodID(
            omniTaskWrapperClass, "closeSavepointInputStream", "(Lorg/apache/flink/core/fs/FSDataInputStream;)V");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: closeSavepointInputStream could not GetMethodID for JNI call");
            throw std::runtime_error("closeSavepointInputStream could not GetMethodID for JNI call");
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteGlobalRef(inputStream);
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream close method");
            throw std::runtime_error("Failed to call SavepointInputStream close method");
        }
        env->DeleteLocalRef(omniTaskWrapperClass);
        env->DeleteGlobalRef(inputStream);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

jobject OmniTaskBridgeImpl2::AcquireSavepointOutputStream(long checkpointId, CheckpointOptions* checkpointOptions)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside AcquireSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside AcquireSavepointOutputStream");
    }
    if (checkpointOptions == nullptr) {
        INFO_RELEASE("Error: checkpointOptions is nullptr in TaskStateManagerBridgeImpl::AcquireSavepointOutputStream");
        throw std::runtime_error(
            "checkpointOptions is nullptr in TaskStateManagerBridgeImpl::AcquireSavepointOutputStream");
    }
    nlohmann::json jcheckpointOptions = checkpointOptions->ToJson();
    std::string checkpointOptionsStr = jcheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "acquireSavepointOutputStream",
        "(JLjava/lang/String;)Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;");
    env->DeleteLocalRef(cls);
    jstring jcheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    auto localProvider = env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId, jcheckpointOptionsStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jcheckpointOptionsStr);
        INFO_RELEASE("Error: Failed to call AcquireSavepointOutputStream");
        throw std::runtime_error("Failed to call AcquireSavepointOutputStream");
    }
    env->DeleteLocalRef(jcheckpointOptionsStr);
    if (localProvider == nullptr) {
        return nullptr;
    }
    // CallObjectMethod 返回 local ref，仅在当前 JNI frame、当前线程有效。
    // CheckpointStateOutputStreamProxy 会把 provider 跨线程（async checkpoint thread）
    // 持续使用，必须升级为 global ref；仅由终态 Close/Abort 操作 DeleteGlobalRef 释放。
    jobject globalProvider = env->NewGlobalRef(localProvider);
    env->DeleteLocalRef(localProvider);
    return globalProvider;
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> OmniTaskBridgeImpl2::CloseSavepointOutputStream(jobject provider)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside CloseSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside CloseSavepointOutputStream");
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "closeSavepointOutputStream",
        "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)Lorg/apache/"
        "flink/runtime/state/SnapshotResult;");
    env->DeleteLocalRef(cls);
    jobject javaResult = env->CallObjectMethod(m_globalOmniTaskRef, mid, provider);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteGlobalRef(provider);
        INFO_RELEASE("Error: Failed to call CloseSavepointOutputStream");
        throw std::runtime_error("Failed to call CloseSavepointOutputStream");
    }
    try {
        auto res = ConvertSnapshotResult(env, javaResult);
        if (javaResult != nullptr) {
            env->DeleteLocalRef(javaResult);
        }
        env->DeleteGlobalRef(provider);
        return res;
    } catch (...) {
        if (javaResult != nullptr) {
            env->DeleteLocalRef(javaResult);
        }
        env->DeleteGlobalRef(provider);
        ERROR_RELEASE("Failed to convert CloseSavepointOutputStream result");
        throw;
    }
}

void OmniTaskBridgeImpl2::AbortSavepointOutputStream(jobject provider)
{
    JNIEnv* env = nullptr;
    const jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    const jint attachRes =
        ret == JNI_EDETACHED ? g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) : JNI_OK;
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside AbortSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside AbortSavepointOutputStream");
    }
    jclass providerClass = env->GetObjectClass(provider);
    if (providerClass == nullptr) {
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        env->DeleteGlobalRef(provider);
        INFO_RELEASE("Error: Failed to get savepoint output provider class");
        throw std::runtime_error("Failed to get savepoint output provider class");
    }
    jmethodID closeMethod = env->GetMethodID(providerClass, "close", "()V");
    env->DeleteLocalRef(providerClass);
    if (closeMethod == nullptr || env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteGlobalRef(provider);
        INFO_RELEASE("Error: Failed to get savepoint output provider close method");
        throw std::runtime_error("Failed to get savepoint output provider close method");
    }
    env->CallVoidMethod(provider, closeMethod);
    const bool closeFailed = env->ExceptionCheck();
    if (closeFailed) {
        env->ExceptionDescribe();
        env->ExceptionClear();
    }
    env->DeleteGlobalRef(provider);
    if (closeFailed) {
        INFO_RELEASE("Error: Failed to close unfinalized savepoint output stream");
        throw std::runtime_error("Failed to close unfinalized savepoint output stream");
    }
}

void OmniTaskBridgeImpl2::WriteSavepointOutputStream(jobject provider, const int8_t* chunk, size_t offset, size_t len)
{
    if (len == 0) {
        return;
    }
    if (len > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
        INFO_RELEASE("Error: Savepoint output chunk len=" << len << " exceeds JNI byte array limit");
        throw std::runtime_error("Savepoint output chunk is larger than JNI byte array limit");
    }
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointOutputStream");
    }
    static jmethodID mid = nullptr;
    if (mid == nullptr) {
        jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
        mid = env->GetMethodID(
            cls,
            "writeSavepointOutputStream",
            "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;[B)V");
        env->DeleteLocalRef(cls);
        if (mid == nullptr) {
            INFO_RELEASE("Error: Failed to find writeSavepointOutputStream method in WriteSavepointOutputStream");
            throw std::runtime_error("Failed to find WriteSavepointOutputStream method");
        }
    }
    jbyteArray data = env->NewByteArray(static_cast<jsize>(len));
    if (data == nullptr || env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        INFO_RELEASE("Error: Failed to allocate savepoint output byte array, len=" << len);
        throw std::runtime_error("Failed to allocate savepoint output byte array");
    }
    env->SetByteArrayRegion(data, 0, static_cast<jsize>(len), reinterpret_cast<const jbyte*>(chunk + offset));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(data);
        INFO_RELEASE("Error: Failed to copy savepoint output byte array, len=" << len);
        throw std::runtime_error("Failed to copy savepoint output byte array");
    }
    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, data);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(data);
        INFO_RELEASE("Error: Failed to call WriteSavepointOutputStream");
        throw std::runtime_error("Failed to call WriteSavepointOutputStream");
    }
    env->DeleteLocalRef(data);
}

jobject OmniTaskBridgeImpl2::CreateSavepointOutputDirectBuffer(void* data, size_t capacity)
{
    if (data == nullptr || capacity == 0) {
        return nullptr;
    }
    static constexpr size_t MAX_DIRECT_BUFFER_SIZE = 4 * 1024 * 1024;
    if (capacity > MAX_DIRECT_BUFFER_SIZE) {
        INFO_RELEASE(
            "Error: Savepoint direct buffer capacity " << capacity << " exceeds max limit " << MAX_DIRECT_BUFFER_SIZE);
        throw std::runtime_error("Savepoint direct buffer capacity exceeds maximum allowed limit");
    }
    if (capacity > static_cast<size_t>(std::numeric_limits<jlong>::max())) {
        INFO_RELEASE("Error: Savepoint direct buffer capacity " << capacity << " exceeds JNI limit");
        throw std::runtime_error("Savepoint direct buffer capacity is larger than JNI direct buffer limit");
    }
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        throw std::runtime_error("Failed to attach C++ thread to JVM inside CreateSavepointOutputDirectBuffer");
    }
    jobject localBuffer = env->NewDirectByteBuffer(data, static_cast<jlong>(capacity));
    if (localBuffer == nullptr || env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return nullptr;
    }
    jobject globalBuffer = env->NewGlobalRef(localBuffer);
    env->DeleteLocalRef(localBuffer);
    if (globalBuffer == nullptr || env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return nullptr;
    }
    return globalBuffer;
}

void OmniTaskBridgeImpl2::ReleaseSavepointOutputDirectBuffer(jobject directBuffer)
{
    if (directBuffer == nullptr) {
        return;
    }
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE(
            "Warning: Failed to attach JNI thread in ReleaseSavepointOutputDirectBuffer, DirectByteBuffer "
            "global ref may leak");
        return;
    }
    env->DeleteGlobalRef(directBuffer);
}

bool OmniTaskBridgeImpl2::WriteSavepointOutputStreamDirect(jobject provider, jobject directBuffer, size_t len)
{
    if (len == 0) {
        return true;
    }
    if (directBuffer == nullptr) {
        throw std::runtime_error("Savepoint output DirectByteBuffer is null");
    }
    if (len > static_cast<size_t>(std::numeric_limits<jint>::max())) {
        throw std::runtime_error("Savepoint output chunk is larger than Java int limit");
    }
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointOutputStreamDirect");
    }
    static jmethodID mid = nullptr;
    if (mid == nullptr) {
        jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
        mid = env->GetMethodID(
            cls,
            "writeSavepointOutputStreamDirect",
            "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;Ljava/nio/ByteBuffer;I)Z");
        env->DeleteLocalRef(cls);
        if (mid == nullptr) {
            if (env->ExceptionCheck()) {
                env->ExceptionDescribe();
                env->ExceptionClear();
            }
            void* address = env->GetDirectBufferAddress(directBuffer);
            if (address == nullptr) {
                throw std::runtime_error("Failed to find WriteSavepointOutputStreamDirect method");
            }
            WriteSavepointOutputStream(provider, reinterpret_cast<const int8_t*>(address), 0, len);
            return false;
        }
    }
    jboolean directWrite =
        env->CallBooleanMethod(m_globalOmniTaskRef, mid, provider, directBuffer, static_cast<jint>(len));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to call WriteSavepointOutputStreamDirect");
    }
    return directWrite == JNI_TRUE;
}

void OmniTaskBridgeImpl2::WriteSavepointMetadata(
    jobject provider, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots, std::string keySerializer)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
    }

    nlohmann::json stateMetaInfoJson = nlohmann::json::array();
    // TODO 需要增加key序列化器和namespace序列化器以保证omnistream创建的savepoint在flink可恢复
    for (const auto& snapshot : snapshots) {
        nlohmann::json jsonObj;
        jsonObj[StateMetaInfoSnapshot::NAME_KEY] = snapshot->getName();
        jsonObj[StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY] =
            static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj[StateMetaInfoSnapshot::OPTIONS_KEY] = snapshot->getOptionsImmutable();
        jsonObj[StateMetaInfoSnapshot::SERIALIZER_KEY] = snapshot->getSerializerJson();
        jsonObj[StateMetaInfoSnapshot::KEYED_BACKEND_KEY_SERIALIZER_KEY] = keySerializer;
        stateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string stateMetaInfoStr = stateMetaInfoJson.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "writeSavepointMetadata",
        "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;Ljava/lang/String;)V");
    env->DeleteLocalRef(cls);
    jstring jStateMetaInfoStr = env->NewStringUTF(stateMetaInfoStr.c_str());
    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, jStateMetaInfoStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jStateMetaInfoStr);
        INFO_RELEASE("Error: Failed to call WriteSavepointMetadata");
        throw std::runtime_error("Failed to call WriteSavepointMetadata");
    }
    env->DeleteLocalRef(jStateMetaInfoStr);
}

void OmniTaskBridgeImpl2::WriteOperatorMetaData(
    jobject provider,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteOperatorMetaData");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteOperatorMetaData");
    }

    nlohmann::json operatorStateMetaInfoJson = nlohmann::json::array();
    nlohmann::json broadcastStateMetaInfoJson = nlohmann::json::array();

    for (const auto& snapshot : operatorStateMetaInfoSnapshots) {
        if (snapshot == nullptr) {
            continue;
        }
        nlohmann::json jsonObj;
        jsonObj[StateMetaInfoSnapshot::NAME_KEY] = snapshot->getName();
        jsonObj[StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY] =
            static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj[StateMetaInfoSnapshot::OPTIONS_KEY] = snapshot->getOptionsImmutable();
        jsonObj[StateMetaInfoSnapshot::SERIALIZER_KEY] = snapshot->getSerializerJson();
        operatorStateMetaInfoJson.push_back(std::move(jsonObj));
    }

    for (const auto& snapshot : broadcastStateMetaInfoSnapshots) {
        if (snapshot == nullptr) {
            continue;
        }
        nlohmann::json jsonObj;
        jsonObj[StateMetaInfoSnapshot::NAME_KEY] = snapshot->getName();
        jsonObj[StateMetaInfoSnapshot::BACKEND_STATE_TYPE_KEY] =
            static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj[StateMetaInfoSnapshot::OPTIONS_KEY] = snapshot->getOptionsImmutable();
        jsonObj[StateMetaInfoSnapshot::SERIALIZER_KEY] = snapshot->getSerializerJson();
        broadcastStateMetaInfoJson.push_back(std::move(jsonObj));
    }

    std::string operatorStateMetaInfoStr = operatorStateMetaInfoJson.dump();
    std::string broadcastStateMetaInfoStr = broadcastStateMetaInfoJson.dump();

    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    if (cls == nullptr) {
        INFO_RELEASE("Error: Failed to get OmniTaskWrapper class inside WriteOperatorMetaData");
        throw std::runtime_error("Failed to get OmniTaskWrapper class inside WriteOperatorMetaData");
    }
    jmethodID mid = env->GetMethodID(
        cls,
        "writeOperatorMetaData",
        "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;Ljava/lang/String;Ljava/lang/"
        "String;)V");
    if (mid == nullptr) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(cls);
        INFO_RELEASE("Error: Failed to get writeOperatorMetaData method inside WriteOperatorMetaData");
        throw std::runtime_error("Failed to get writeOperatorMetaData method inside WriteOperatorMetaData");
    }

    jstring jOperatorStateMetaInfoStr = env->NewStringUTF(operatorStateMetaInfoStr.c_str());
    jstring jBroadcastStateMetaInfoStr = env->NewStringUTF(broadcastStateMetaInfoStr.c_str());

    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, jOperatorStateMetaInfoStr, jBroadcastStateMetaInfoStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jOperatorStateMetaInfoStr);
        env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
        env->DeleteLocalRef(cls);
        INFO_RELEASE("Error: Failed to call WriteOperatorMetaData");
        throw std::runtime_error("Failed to call WriteOperatorMetaData");
    }
    env->DeleteLocalRef(jOperatorStateMetaInfoStr);
    env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
    env->DeleteLocalRef(cls);
}

long OmniTaskBridgeImpl2::GetSavepointOutputStreamPos(jobject provider)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside GetSavepointOutputStreamPos");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside GetSavepointOutputStreamPos");
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls, "getSavepointOutputStreamPos", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)J");
    env->DeleteLocalRef(cls);
    auto pos = env->CallLongMethod(m_globalOmniTaskRef, mid, provider);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        // Position lookup is non-terminal: the proxy retains provider so the
        // writer can abort it exactly once during error cleanup.
        INFO_RELEASE("Error: Failed to call GetSavepointOutputStreamPos");
        throw std::runtime_error("Failed to call GetSavepointOutputStreamPos");
    }
    return pos;
}

JNIEnv* OmniTaskBridgeImpl2::getJNIEnv()
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallUploadFilesToCheckpointFs");
        return nullptr;
    }
    return env;
}
