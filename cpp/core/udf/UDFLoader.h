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

#ifndef FLINK_TNEL_UDFLOADER_H
#define FLINK_TNEL_UDFLOADER_H

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include "dlfcn.h"
#include "functions/MapFunction.h"
#include "functions/ReduceFunction.h"
#include "functions/FilterFunction.h"
#include "functions/SourceFunction.h"
#include "functions/FlatMapFunction.h"
#include "functions/KeySelect.h"
#include "functions/KeyedCoProcessFunction.h"
#include "streaming/api/functions/ProcessFunction.h"
#include "functions/RuntimeContext.h"
#include "udf_hash.h"
#include "nlohmann/json.hpp"

using MapDllType = MapFunctionUnique<Object>(nlohmann::json);
using ReduceDllType = ReduceFunctionUnique<Object>(nlohmann::json);
using FlatMapDllType = FlatMapFunctionUnique<Object>(nlohmann::json);
using FilterDllType = FilterFunctionUnique<Object>(nlohmann::json);
using SourceDllType = SourceFunctionUnique<Object>(nlohmann::json);
using KeySelectDllType = KeySelectUnique<Object>(nlohmann::json);
using KeyedCoProcessDllType = KeyedCoProcessFunctionUnique<Object*, Object*, Object*, Object*>(nlohmann::json);
using ProcessOperatorDllType = ProcessFunctionUnique<Object*, Object*>(nlohmann::json);

using SerializeFunction = char*(char*);
using DeSerializeFunction = char*(char*);
using DebugFunction = void(char*);

using RichMapFunctionType = MapFunctionUnique<Object>(RuntimeContext*);

enum class UDFLogicType : uint8_t {
    Map,
    Filter,
    RichMap,
    RichFilter,
    Serialize,
    DeSerialize,
    Reduce,
    Hash,
    Cmp,
};

class UDFLoader {
public:
    MapDllType* LoadMapFunction(const std::string& filePath)
    {
        return LoadUDFFunction<MapDllType>(filePath, MapFuncName);
    }

    FlatMapDllType* LoadFlatMapFunction(const std::string& filePath)
    {
        return LoadUDFFunction<FlatMapDllType>(filePath, FlatMapFuncName);
    }

    FilterDllType* LoadFilterFunction(const std::string& filePath)
    {
        return LoadUDFFunction<FilterDllType>(filePath, FilterFuncName);
    }

    ReduceDllType* LoadReduceFunction(const std::string& filePath)
    {
        return LoadUDFFunction<ReduceDllType>(filePath, ReduceFuncName);
    }

    SerializeFunction* LoadSerFunction(const std::string& filePath)
    {
        return LoadUDFFunction<SerializeFunction>(filePath, SerializeName);
    }

    SerializeFunction* LoadDeSerFunction(const std::string& filePath)
    {
        return LoadUDFFunction<DeSerializeFunction>(filePath, DeSerializeName);
    }

    DebugFunction* LoadDebugFunction(const std::string& filePath)
    {
        return LoadUDFFunction<DebugFunction>(filePath, DebugName);
    }

    HashFunctionType* LoadHashFunction(const std::string& filePah)
    {
        return LoadUDFFunction<HashFunctionType>(filePah, HashName);
    }

    CmpFunctionType* LoadCmpFunction(const std::string& filePah)
    {
        return LoadUDFFunction<CmpFunctionType>(filePah, CmpName);
    }

    SourceDllType* LoadSourceFunction(const std::string& filePath)
    {
        return LoadUDFFunction<SourceDllType>(filePath, SourceFuncName);
    }

    KeySelectDllType* LoadKeySelectFunction(const std::string& filePath)
    {
        return LoadUDFFunction<KeySelectDllType>(filePath, KeySelectName);
    }

    KeyedCoProcessDllType* LoadKeyedCoProcessFunction(const std::string& filePath)
    {
        return LoadUDFFunction<KeyedCoProcessDllType>(filePath, KeyedCoProcessFuncName);
    }

    ProcessOperatorDllType* LoadProcessOperatorFunction(const std::string& filePath)
    {
        return LoadUDFFunction<ProcessOperatorDllType>(filePath, ProcessOperatorFuncName);
    }

    RichMapFunctionType* LoadRichMapFunction(const std::string& filePah)
    {
        return LoadUDFFunction<RichMapFunctionType>(filePah, HashName);
    }

private:
    struct FreeDeleter {
        void operator()(char* value) const noexcept
        {
            std::free(value);
        }
    };

    static bool IsTrustedOwner(const struct stat& st)
    {
        return st.st_uid == 0 || st.st_uid == geteuid();
    }

    static bool IsTrustedDirectory(const struct stat& st)
    {
        if (!S_ISDIR(st.st_mode) || !IsTrustedOwner(st)) {
            return false;
        }
        const bool writableByOthers = (st.st_mode & (S_IWGRP | S_IWOTH)) != 0;
        // /tmp 一类 sticky 目录不允许其他用户替换当前用户拥有的目录项。
        return !writableByOthers || (st.st_mode & S_ISVTX) != 0;
    }

    // 允许任意受保护目录中的 SO，兼容 Flink 动态生成的 blobStorage 路径。
    // 文件及目录链必须属于 root/当前用户；文件不可被组或其他用户改写。
    static bool IsTrustedUdfPath(const std::string& realPath)
    {
        struct stat st{};
        if (stat(realPath.c_str(), &st) != 0 || !S_ISREG(st.st_mode) || !IsTrustedOwner(st) ||
            (st.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
            return false;
        }

        size_t separator = realPath.rfind('/');
        std::string directory = separator == 0 ? "/" : realPath.substr(0, separator);
        while (!directory.empty()) {
            if (stat(directory.c_str(), &st) != 0 || !IsTrustedDirectory(st)) {
                return false;
            }
            if (directory == "/") {
                break;
            }
            separator = directory.rfind('/');
            directory = separator == 0 ? "/" : directory.substr(0, separator);
        }
        return true;
    }

    template <typename FuncType>
    FuncType* LoadUDFFunction(const std::string& filePath, const std::string& funcSignature)
    {
        if (filePath.empty() || filePath.find('\0') != std::string::npos) {
            std::cerr << "Error: rejected invalid UDF library path" << std::endl;
            return nullptr;
        }

        // 由 realpath 分配结果，消解符号链接和 ".."，避免固定长度输出缓冲。
        std::unique_ptr<char, FreeDeleter> resolved(realpath(filePath.c_str(), nullptr));
        if (resolved == nullptr) {
            std::cerr << "Error: cannot resolve UDF library path: " << filePath << std::endl;
            return nullptr;
        }
        const std::string realPath(resolved.get());
        if (!IsTrustedUdfPath(realPath)) {
            std::cerr << "Error: rejected untrusted UDF library path: " << filePath << std::endl;
            return nullptr;
        }

        void* handle = dlopen(realPath.c_str(), RTLD_LAZY);
        if (not handle) {
            std::cerr << "Error loading library: " << dlerror() << std::endl;
            return nullptr;
        }

        FuncType* funcPointer = (FuncType*)dlsym(handle, funcSignature.c_str());

        const char* path = nullptr;
        if (path == nullptr) {
            Dl_info info;
            if (dladdr((void*)funcPointer, &info)) {
                path = info.dli_fname;
            } else {
                path = "[Error] Failed to get SO path";
            }
        }
        std::cout << "so path: " << path << std::endl;

        const char* error = dlerror();
        if (error) {
            std::cerr << "Error finding symbol: " << error << std::endl;
            dlclose(handle);
            return nullptr;
        }
        return funcPointer;
    }

    const char* NormalFunctionName = "NewInstance";
    const char* ReduceFuncName = NormalFunctionName;
    const char* MapFuncName = NormalFunctionName;
    const char* SerializeName = NormalFunctionName;
    const char* DeSerializeName = NormalFunctionName;
    const char* FlatMapFuncName = NormalFunctionName;
    const char* FilterFuncName = NormalFunctionName;
    const char* SourceFuncName = NormalFunctionName;
    const char* KeySelectName = NormalFunctionName;
    const char* KeyedCoProcessFuncName = NormalFunctionName;
    const char* ProcessOperatorFuncName = NormalFunctionName;
    const char* DebugName = NormalFunctionName;
    const char* HashName = "Hash";
    const char* CmpName = "Cmp";
};
#endif
