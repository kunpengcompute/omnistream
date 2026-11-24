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

#include "RocksDBStateDownloader.h"
#include "common/global.h"
#include <stdexcept>

jobject convertToJavaByteStreamStateHandle(JNIEnv* env, const ByteStreamStateHandle& cppHandle)
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

jobject convertToJavaFileStateHandle(JNIEnv* env, const FileStateHandle& cppHandle)
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

jobject convertToJavaRelativeFileStateHandle(JNIEnv* env, const RelativeFileStateHandle& cppHandle)
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

jobject convertToJavaStreamStateHandle(JNIEnv* env, const StreamStateHandle& cppHandle)
{
    // 动态类型检查（如果是 ByteStreamStateHandle）
    if (auto byteHandle = dynamic_cast<const ByteStreamStateHandle*>(&cppHandle)) {
        return convertToJavaByteStreamStateHandle(env, *byteHandle);
    } else if (auto fileHandle = dynamic_cast<const FileStateHandle*>(&cppHandle)) {
        return convertToJavaFileStateHandle(env, *fileHandle);
    } else if (auto relHandle = dynamic_cast<const RelativeFileStateHandle*>(&cppHandle)) {
        return convertToJavaRelativeFileStateHandle(env, *relHandle);
    } else {
        env->ThrowNew(env->FindClass("java/lang/UnsupportedOperationException"), "Unsupported StreamStateHandle type");
        return nullptr;
    }
}

// 转换C++ HandleAndLocalPath为Java HandleAndLocalPath
jobject convertToJavaIncrementalHandleAndLocalPath(
    JNIEnv* env, const IncrementalKeyedStateHandle::HandleAndLocalPath& cppHandle)
{
    // 1. 获取Java嵌套类和方法
    jclass incrementalHandleClass = env->FindClass("org/apache/flink/runtime/state/IncrementalKeyedStateHandle");
    if (!incrementalHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }
    // 获取嵌套类HandleAndLocalPath
    jclass handleAndPathClass =
        env->FindClass("org/apache/flink/runtime/state/IncrementalKeyedStateHandle$HandleAndLocalPath");
    if (!handleAndPathClass) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(incrementalHandleClass);
        return nullptr;
    }
    // 获取工厂方法ID
    jmethodID factoryMethod = env->GetStaticMethodID(
        handleAndPathClass,
        "of",
        "(Lorg/apache/flink/runtime/state/StreamStateHandle;Ljava/lang/String;)"
        "Lorg/apache/flink/runtime/state/IncrementalKeyedStateHandle$HandleAndLocalPath;");
    if (!factoryMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleAndPathClass);
        env->DeleteLocalRef(incrementalHandleClass);
        return nullptr;
    }
    // 2. 转换StreamStateHandle
    jobject javaStateHandle = nullptr;
    if (cppHandle.getHandle()) {
        javaStateHandle = convertToJavaStreamStateHandle(env, *cppHandle.getHandle());
        if (!javaStateHandle) {
            env->DeleteLocalRef(handleAndPathClass);
            env->DeleteLocalRef(incrementalHandleClass);
            return nullptr;
        }
    }
    // 3. 转换localPath
    jstring jLocalPath = env->NewStringUTF(cppHandle.getLocalPath().c_str());
    if (!jLocalPath) {
        if (javaStateHandle) env->DeleteLocalRef(javaStateHandle);
        env->DeleteLocalRef(handleAndPathClass);
        env->DeleteLocalRef(incrementalHandleClass);
        return nullptr;
    }
    // 4. 调用工厂方法创建Java对象
    jobject javaHandleAndPath =
        env->CallStaticObjectMethod(handleAndPathClass, factoryMethod, javaStateHandle, jLocalPath);
    // 5. 清理局部引用
    if (javaStateHandle) env->DeleteLocalRef(javaStateHandle);
    env->DeleteLocalRef(jLocalPath);
    env->DeleteLocalRef(handleAndPathClass);
    env->DeleteLocalRef(incrementalHandleClass);

    return javaHandleAndPath;
}

// 转换vector<HandleAndLocalPath>为Java List
jobject convertToJavaHandleAndLocalPathList(JNIEnv* env, const std::vector<HandleAndLocalPath>& cppHandles)
{
    // 1. 获取ArrayList类和方法
    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    jmethodID constructor = env->GetMethodID(arrayListClass, "<init>", "()V");
    jmethodID addMethod = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");

    // 2. 创建ArrayList
    jobject javaList = env->NewObject(arrayListClass, constructor);
    if (!javaList) {
        env->DeleteLocalRef(arrayListClass);
        return nullptr;
    }

    // 3. 填充数据
    for (const auto& cppHandle : cppHandles) {
        jobject javaItem = convertToJavaIncrementalHandleAndLocalPath(env, cppHandle);
        if (!javaItem) {
            env->DeleteLocalRef(javaList);
            env->DeleteLocalRef(arrayListClass);
            return nullptr;
        }

        env->CallBooleanMethod(javaList, addMethod, javaItem);
        env->DeleteLocalRef(javaItem);
    }

    // 4. 清理并返回
    env->DeleteLocalRef(arrayListClass);
    return javaList;
}

jobject convertFsPathToJavaPath(JNIEnv* env, const fs::path& restoreInstancePath)
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

void RocksDBStateDownloader::callDownloadDataForAllStateHandles(
    const std::vector<HandleAndLocalPath>& handleWithPaths,
    const fs::path& restoreInstancePath,
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge)
{
    auto env = omniTaskBridge->getJNIEnv();
    // 1. 查找类
    jclass downloaderClass = env->FindClass("org/apache/flink/contrib/streaming/state/RocksDBStateDownloader");
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find RocksDBStateUploader class");
    }

    // 2. 获取构造函数方法ID
    jmethodID constructor = env->GetMethodID(downloaderClass, "<init>", "(I)V");
    jobject downloaderInstance = env->NewObject(downloaderClass, constructor, restoringThreadNum_);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to NewObject");
    }

    //
    jobject jPath = convertFsPathToJavaPath(env, restoreInstancePath);

    //
    jobject javaHandleAndLocalPathList = convertToJavaHandleAndLocalPathList(env, handleWithPaths);

    // 转换CloseableRegistry
    jclass closeableClass = env->FindClass("org/apache/flink/core/fs/CloseableRegistry");
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find CloseableRegistry class");
    }
    jmethodID closeableCtor = env->GetMethodID(closeableClass, "<init>", "()V");
    jobject jCloseableRegistry = env->NewObject(closeableClass, closeableCtor);

    // 查找目标方法
    jmethodID downloadMethod = env->GetMethodID(
        downloaderClass,
        "downloadDataForAllStateHandles",
        "(Ljava/util/List;Ljava/nio/file/Path;"
        "Lorg/apache/flink/core/fs/CloseableRegistry;)V");

    env->CallVoidMethod(downloaderInstance, downloadMethod, javaHandleAndLocalPathList, jPath, jCloseableRegistry);

    env->DeleteLocalRef(closeableClass);
    env->DeleteLocalRef(downloaderInstance);
    env->DeleteLocalRef(javaHandleAndLocalPathList);
    env->DeleteLocalRef(jPath);
    env->DeleteLocalRef(jCloseableRegistry);
    env->DeleteLocalRef(downloaderClass);
}

RocksDBStateDownloader::RocksDBStateDownloader(int restoringThreadNum)
    : RocksDBStateDataTransfer(restoringThreadNum),
      restoringThreadNum_(restoringThreadNum)
{
}
