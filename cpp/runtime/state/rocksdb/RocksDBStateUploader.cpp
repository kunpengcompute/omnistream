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

#include "RocksDBStateUploader.h"
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

std::string jstringToString(JNIEnv* env, jstring jstr)
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

std::string flinkPathToString(JNIEnv* env, jobject flinkPathObj)
{
    if (!env || !flinkPathObj) {
        return "";
    }

    // 获取Flink Path类引用
    const char* pathClassPath = "org/apache/flink/core/fs/Path";
    jclass pathClass = env->FindClass(pathClassPath);
    if (!pathClass) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find org.apache.flink.core.fs.Path class");
    }

    // 调用toString()方法
    jmethodID getPathMethod = env->GetMethodID(pathClass, "toString", "()Ljava/lang/String;");
    if (!getPathMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to get Path.toString() method ID");
    }

    // 调用getPath()方法获取字符串路径
    jstring pathStr = static_cast<jstring>(env->CallObjectMethod(flinkPathObj, getPathMethod));

    // 检查异常
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(pathStr);
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to call Path.getPath() method");
    }

    // 转换为C++字符串
    std::string result = jstringToString(env, pathStr);

    // 释放资源
    env->DeleteLocalRef(pathStr);
    env->DeleteLocalRef(pathClass);

    return result;
}

jobject RocksDBStateUploader::addToJavaPathList(
    JNIEnv* env,
    const std::vector<fs::path>& files,
    jobject javaList,
    jmethodID arrayListAdd,
    jclass pathsClass,
    jmethodID pathsGet)
{
    jclass stringClass = env->FindClass("java/lang/String");
    if (!stringClass) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        ERROR_RELEASE("Failed to find java.lang.String class");
        throw std::runtime_error("Failed to find java.lang.String class");
    }
    for (const auto& cppPath : files) {
        // 转换为Java String
        std::string pathStr = cppPath.string();
        jstring javaStr = env->NewStringUTF(pathStr.c_str());
        if (!javaStr) {
            env->ExceptionClear();
            std::cerr << "Failed to create Java String for path: " << pathStr << std::endl;
            continue;
        }

        // 创建空的String数组（用于可变参数）
        jobjectArray moreParts = env->NewObjectArray(0, stringClass, nullptr);
        if (!moreParts) {
            env->ExceptionClear();
            env->DeleteLocalRef(javaStr);
            continue;
        }

        // 创建Java Path对象
        jobject javaPath = env->CallStaticObjectMethod(pathsClass, pathsGet, javaStr, moreParts);
        if (env->ExceptionCheck() || !javaPath) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteLocalRef(javaStr);
            env->DeleteLocalRef(moreParts);
            continue;
        }

        // 添加到列表
        env->CallBooleanMethod(javaList, arrayListAdd, javaPath);

        // 释放局部引用
        env->DeleteLocalRef(javaStr);
        env->DeleteLocalRef(moreParts);
        env->DeleteLocalRef(javaPath);
    }

    env->DeleteLocalRef(stringClass);

    return javaList;
}

// 将C++的fs::path列表转换为Java的List<Path>对象
jobject RocksDBStateUploader::createJavaPathList(JNIEnv* env, const std::vector<fs::path>& files)
{
    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    if (!arrayListClass) {
        std::cerr << "Failed to find ArrayList class" << std::endl;
        return nullptr;
    }

    jmethodID arrayListCtor = env->GetMethodID(arrayListClass, "<init>", "()V");
    jmethodID arrayListAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
    if (!arrayListCtor || !arrayListAdd) {
        std::cerr << "Failed to get ArrayList methods" << std::endl;
        env->DeleteLocalRef(arrayListClass);
        return nullptr;
    }

    jclass pathsClass = env->FindClass("java/nio/file/Paths");
    if (!pathsClass) {
        std::cerr << "Failed to find Paths class" << std::endl;
        env->DeleteLocalRef(arrayListClass);
        return nullptr;
    }
    jmethodID pathsGet =
        env->GetStaticMethodID(pathsClass, "get", "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;");
    if (!pathsGet) {
        std::cerr << "Failed to find Paths.get method" << std::endl;
        env->DeleteLocalRef(arrayListClass);
        env->DeleteLocalRef(pathsClass);
        return nullptr;
    }

    // 2. 创建ArrayList实例
    jobject javaList = env->NewObject(arrayListClass, arrayListCtor);
    if (!javaList) {
        std::cerr << "Failed to create ArrayList" << std::endl;
        env->DeleteLocalRef(arrayListClass);
        env->DeleteLocalRef(pathsClass);
        return nullptr;
    }

    // 3. 遍历转换并添加路径
    try {
        javaList = addToJavaPathList(env, files, javaList, arrayListAdd, pathsClass, pathsGet);
    } catch (...) {
        env->DeleteLocalRef(javaList);
        env->DeleteLocalRef(arrayListClass);
        env->DeleteLocalRef(pathsClass);
        ERROR_RELEASE("Failed to add paths to Java path list");
        throw;
    }

    // 4. 释放临时引用
    env->DeleteLocalRef(arrayListClass);
    env->DeleteLocalRef(pathsClass);

    return javaList;
}

StreamStateHandleType getHandleType(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        return StreamStateHandleType::Unknown;
    }

    // 子类完整类路径（需根据实际包路径调整）
    const char* byteStreamClassPath = "org/apache/flink/runtime/state/memory/ByteStreamStateHandle";
    const char* relativeFileClassPath = "org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle";
    const char* fileClassPath = "org/apache/flink/runtime/state/filesystem/FileStateHandle";

    // 获取类引用
    jclass byteStreamClass = env->FindClass(byteStreamClassPath);
    jclass relativeFileClass = env->FindClass(relativeFileClassPath);
    jclass fileClass = env->FindClass(fileClassPath);

    // 检查类引用是否有效
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

    // 检查异常
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

/**
 * 从FileStateHandle获取属性并创建C++对象
 */
std::shared_ptr<StreamStateHandle> createFileStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    // 获取类引用
    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get FileStateHandle class");
    }

    // 获取get方法ID
    jmethodID getFilePathMethod = env->GetMethodID(handleClass, "getFilePath", "()Lorg/apache/flink/core/fs/Path;");
    jmethodID getStateSizeMethod = env->GetMethodID(handleClass, "getStateSize", "()J");
    if (!getFilePathMethod || !getStateSizeMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get FileStateHandle method IDs");
    }

    // 调用get方法获取属性值
    jobject jFilePath = env->CallObjectMethod(handleObj, getFilePathMethod);
    jlong jStateSize = env->CallLongMethod(handleObj, getStateSizeMethod);

    // 检查方法调用异常
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jFilePath) env->DeleteLocalRef(jFilePath);
        throw std::runtime_error("Failed to call FileStateHandle get methods");
    }

    // 转换属性值
    std::string filePathStr = flinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    // 释放资源
    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<FileStateHandle>(filePath, stateSize);
}

/**
 * 从RelativeFileStateHandle获取属性并创建C++对象
 */
std::shared_ptr<StreamStateHandle> createRelativeFileStateHandle(JNIEnv* env, jobject handleObj)
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

    // 转换Path对象
    std::string filePathStr = flinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    std::string relativePath = jstringToString(env, static_cast<jstring>(jRelativePath));
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(jRelativePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<RelativeFileStateHandle>(filePath, relativePath, stateSize);
}

/**
 * 从ByteStreamStateHandle获取属性并创建C++对象
 */
std::shared_ptr<StreamStateHandle> createByteStreamStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    // 获取类引用
    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get ByteStreamStateHandle class");
    }

    // 获取get方法ID
    jmethodID getHandleNameMethod = env->GetMethodID(handleClass, "getHandleName", "()Ljava/lang/String;");
    jmethodID getDataMethod = env->GetMethodID(handleClass, "getData", "()[B");
    if (!getHandleNameMethod || !getDataMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get ByteStreamStateHandle method IDs");
    }

    // 调用get方法获取属性值
    jobject jHandleName = env->CallObjectMethod(handleObj, getHandleNameMethod);
    jbyteArray jData = static_cast<jbyteArray>(env->CallObjectMethod(handleObj, getDataMethod));

    // 检查方法调用异常
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jHandleName) env->DeleteLocalRef(jHandleName);
        if (jData) env->DeleteLocalRef(jData);
        throw std::runtime_error("Failed to call ByteStreamStateHandle get methods");
    }

    // 转换属性值
    std::string handleName = jstringToString(env, static_cast<jstring>(jHandleName));
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

    // 释放资源
    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<ByteStreamStateHandle>(handleName, data);
}

std::shared_ptr<StreamStateHandle> getStreamStateHandle(JNIEnv* env, jobject jHandleObj, jmethodID getHandleMethod)
{
    std::shared_ptr<StreamStateHandle> handle;
    // 提取 handle 字段
    jobject jHandle = env->CallObjectMethod(jHandleObj, getHandleMethod);
    if (jHandle) {
        auto type = getHandleType(env, jHandle);
        switch (type) {
            case StreamStateHandleType::FileStateHandle: handle = createFileStateHandle(env, jHandle); break;
            case StreamStateHandleType::RelativeFileStateHandle:
                handle = createRelativeFileStateHandle(env, jHandle);
                break;
            case StreamStateHandleType::ByteStreamStateHandle:
                handle = createByteStreamStateHandle(env, jHandle);
                break;
            default:
                env->DeleteLocalRef(jHandle);
                ERROR_RELEASE("Unknown StreamStateHandle type");
                throw std::runtime_error("Unknown StreamStateHandle type");
        }
        env->DeleteLocalRef(jHandle);
    }
    return handle;
}

HandleAndLocalPath convertJavaHandleAndLocalPath(JNIEnv* env, jobject jHandleObj)
{
    std::string localPath;

    // 获取 Java 类引用
    jclass handleClass = env->GetObjectClass(jHandleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get HandleAndLocalPath class reference");
    }
    // 获取字段 ID (根据实际 Java 类的字段名调整)
    jmethodID getHandleMethod =
        env->GetMethodID(handleClass, "getHandle", "()Lorg/apache/flink/runtime/state/StreamStateHandle;");
    jmethodID getLocalPathMethod = env->GetMethodID(handleClass, "getLocalPath", "()Ljava/lang/String;");
    if (!getHandleMethod || !getLocalPathMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get field IDs for HandleAndLocalPath");
    }
    auto handle = getStreamStateHandle(env, jHandleObj, getHandleMethod);
    // 提取 localPath 字段
    jstring jLocalPath = static_cast<jstring>(env->CallObjectMethod(jHandleObj, getLocalPathMethod));
    if (jLocalPath) {
        const char* pathStr = env->GetStringUTFChars(jLocalPath, nullptr);
        if (pathStr) {
            localPath = std::string(pathStr);
            env->ReleaseStringUTFChars(jLocalPath, pathStr);
        }
        env->DeleteLocalRef(jLocalPath);
    }
    auto result = HandleAndLocalPath::of(handle, localPath);
    // 释放类引用
    env->DeleteLocalRef(handleClass);
    return result;
}

std::vector<HandleAndLocalPath> convertJavaListToCppVector(JNIEnv* env, jobject javaList)
{
    std::vector<HandleAndLocalPath> result;
    if (!env || !javaList) {
        return result;
    }
    // 获取 List 类和相关方法 ID
    jclass listClass = env->FindClass("java/util/List");
    if (!listClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to find java.util.List class");
    }
    // 获取 List.size () 方法
    jmethodID sizeMethod = env->GetMethodID(listClass, "size", "()I");
    // 获取 List.get (int) 方法
    jmethodID getMethod = env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
    if (!sizeMethod || !getMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(listClass);
        throw std::runtime_error("Failed to get List method IDs");
    }
    // 获取列表大小
    jint listSize = env->CallIntMethod(javaList, sizeMethod);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(listClass);
        throw std::runtime_error("Failed to call List.size () method");
    }
    // 遍历列表元素并转换
    for (jint i = 0; i < listSize; ++i) {
        jobject element = env->CallObjectMethod(javaList, getMethod, i);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteLocalRef(element);
            env->DeleteLocalRef(listClass);
            throw std::runtime_error("Failed to call List.get () method");
        }
        if (element) {
            // 转换单个元素
            auto cppElement = convertJavaHandleAndLocalPath(env, element);
            result.push_back(cppElement);
            // 释放局部引用
            env->DeleteLocalRef(element);
        }
    }
    // 释放 List 类引用
    env->DeleteLocalRef(listClass);
    return result;
}

std::vector<HandleAndLocalPath> handleCheckpointResult(
    JNIEnv* env,
    CheckpointedStateScope& stateScope,
    jclass uploaderClass,
    jobject uploaderInstance,
    jobject javaFiles,
    jobject jCheckpointStreamFactory)
{
    // 转换CheckpointedStateScope枚举
    jclass scopeClass = env->FindClass("org/apache/flink/runtime/state/CheckpointedStateScope");
    if (!scopeClass) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        ERROR_RELEASE("Failed to find CheckpointedStateScope class");
        throw std::runtime_error("Failed to find CheckpointedStateScope class");
    }
    jfieldID scopeField = env->GetStaticFieldID(
        scopeClass,
        stateScope == CheckpointedStateScope::EXCLUSIVE ? "EXCLUSIVE" : "SHARED",
        "Lorg/apache/flink/runtime/state/CheckpointedStateScope;");
    if (scopeField == nullptr || env->ExceptionCheck()) {
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        env->DeleteLocalRef(scopeClass);
        ERROR_RELEASE("Failed to get CheckpointedStateScope field");
        throw std::runtime_error("Failed to get CheckpointedStateScope field");
    }
    jobject jStateScope = env->GetStaticObjectField(scopeClass, scopeField);

    // 转换CloseableRegistry
    jclass closeableClass = env->FindClass("org/apache/flink/core/fs/CloseableRegistry");
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(scopeClass);
        env->DeleteLocalRef(jStateScope);
        throw std::runtime_error("Failed to find CloseableRegistry class");
    }
    jmethodID closeableCtor = env->GetMethodID(closeableClass, "<init>", "()V");
    jobject jCloseableRegistry = env->NewObject(closeableClass, closeableCtor);
    jobject jTmpResourcesRegistry = env->NewObject(closeableClass, closeableCtor);

    // 4. 查找目标方法
    jmethodID uploadMethod = env->GetMethodID(
        uploaderClass,
        "uploadFilesToCheckpointFs",
        "(Ljava/util/List;"
        "Lorg/apache/flink/runtime/state/CheckpointStreamFactory;"
        "Lorg/apache/flink/runtime/state/CheckpointedStateScope;"
        "Lorg/apache/flink/core/fs/CloseableRegistry;"
        "Lorg/apache/flink/core/fs/CloseableRegistry;)"
        "Ljava/util/List;");

    // 5. 调用Java方法
    jobject resultList = env->CallObjectMethod(
        uploaderInstance,
        uploadMethod,
        javaFiles,
        jCheckpointStreamFactory,
        jStateScope,
        jCloseableRegistry,
        jTmpResourcesRegistry);
    std::vector<HandleAndLocalPath> chkResult;
    try {
        chkResult = convertJavaListToCppVector(env, resultList);
    } catch (...) {
        if (resultList != nullptr) {
            env->DeleteLocalRef(resultList);
        }
        env->DeleteLocalRef(scopeClass);
        env->DeleteLocalRef(jStateScope);
        env->DeleteLocalRef(closeableClass);
        env->DeleteLocalRef(jCloseableRegistry);
        env->DeleteLocalRef(jTmpResourcesRegistry);
        throw;
    }

    if (resultList != nullptr) {
        env->DeleteLocalRef(resultList);
    }
    env->DeleteLocalRef(scopeClass);
    env->DeleteLocalRef(jStateScope);
    env->DeleteLocalRef(closeableClass);
    env->DeleteLocalRef(jCloseableRegistry);
    env->DeleteLocalRef(jTmpResourcesRegistry);
    return chkResult;
}

// 调用Java方法并返回JSON格式结果
std::vector<HandleAndLocalPath> RocksDBStateUploader::callUploadFilesToCheckpointFs(
    JNIEnv* env,
    const std::vector<fs::path>& files,
    CheckpointedStateScope& stateScope,
    jobject jCheckpointStreamFactory)
{
    // 1. 查找Java类
    jclass uploaderClass = env->FindClass("org/apache/flink/contrib/streaming/state/RocksDBStateUploader");
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find RocksDBStateUploader class");
    }

    // 2. 创建RocksDBStateUploader实例
    jmethodID uploaderCtor = env->GetMethodID(uploaderClass, "<init>", "(I)V");
    jobject uploaderInstance = env->NewObject(uploaderClass, uploaderCtor, numberOfSnapshottingThreads_);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(uploaderClass);
        throw std::runtime_error("Failed to NewObject");
    }

    jobject javaFiles = nullptr;
    auto cleanupLocalRefs = [&]() {
        if (javaFiles != nullptr) {
            env->DeleteLocalRef(javaFiles);
            javaFiles = nullptr;
        }
        if (uploaderInstance != nullptr) {
            env->DeleteLocalRef(uploaderInstance);
            uploaderInstance = nullptr;
        }
        if (uploaderClass != nullptr) {
            env->DeleteLocalRef(uploaderClass);
            uploaderClass = nullptr;
        }
    };

    try {
        // 3. 准备方法参数
        javaFiles = createJavaPathList(env, files);
        auto chkResult = handleCheckpointResult(
            env, stateScope, uploaderClass, uploaderInstance, javaFiles, jCheckpointStreamFactory);

        // 检查JNI调用异常
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            ERROR_RELEASE("Exception occurred during RocksDBStateUploader JNI call");
            throw std::runtime_error("Exception occurred during JNI call");
        }

        cleanupLocalRefs();
        return chkResult;
    } catch (...) {
        cleanupLocalRefs();
        ERROR_RELEASE("Failed to upload RocksDB state files to checkpoint filesystem");
        throw;
    }
}

std::vector<HandleAndLocalPath> RocksDBStateUploader::callUploadFilesToCheckpointFs(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge, const std::vector<fs::path>& files)
{
    std::vector<Path> filePaths;
    filePaths.reserve(files.size());
    for (const auto& fs_path : files) {
        filePaths.emplace_back(fs_path.string());
    }

    auto HandleAndLocalPathJobj = bridge->CallUploadFilesToCheckpointFs(filePaths, numberOfSnapshottingThreads_);
    auto env = bridge->getJNIEnv();
    try {
        auto result = convertJavaListToCppVector(env, HandleAndLocalPathJobj);
        if (HandleAndLocalPathJobj != nullptr) {
            env->DeleteLocalRef(HandleAndLocalPathJobj);
        }
        return result;
    } catch (...) {
        if (HandleAndLocalPathJobj != nullptr) {
            env->DeleteLocalRef(HandleAndLocalPathJobj);
        }
        throw;
    }
}

RocksDBStateUploader::RocksDBStateUploader(int numberOfSnapshottingThreads)
    : numberOfSnapshottingThreads_(numberOfSnapshottingThreads)
{
}
