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

#ifndef OMNISTREAM_NATIVETABLEFUNCTIONFACTORY_H
#define OMNISTREAM_NATIVETABLEFUNCTIONFACTORY_H

#include <memory>
#include <string>
#include <unordered_map>
#include <functional>

#include "NativeTableFunction.h"
#include "JsonSplitFunction.h"
#include "common.h"

/**
 * NativeTableFunction 的工厂类。
 *
 * 通过 Java 全限定类名（functionClass）查找对应的 native 实现。
 * 新增 native UDTF 时只需：
 *   1. 在 tablefunction/ 目录下新增 XxxFunction.h
 *   2. 在下面的 registry 中注册映射
 *   3. Java 侧 ValidateCorrelateOPStrategy 白名单中加入 functionClass
 */
class NativeTableFunctionFactory {
public:
    using Creator = std::function<std::unique_ptr<NativeTableFunction>()>;

    /**
     * 根据 Java 全限定类名创建对应的 NativeTableFunction 实例。
     *
     * @param functionClass Java 侧的 TableFunction 全限定类名
     * @return 对应的 native 实现，如果不支持则返回 nullptr
     */
    static std::unique_ptr<NativeTableFunction> create(const std::string& functionName)
    {
        auto& registry = getRegistry();
        auto it = registry.find(functionName);
        if (it != registry.end()) {
            LOG("NativeTableFunctionFactory: creating native impl for " + functionName);
            return it->second();
        }

        // 未找到 native 实现，打印日志
        // 正常情况下不应该走到这里，因为 Java 侧已经做了白名单过滤
        LOG("NativeTableFunctionFactory: no native impl for " + functionName +
            ", this should not happen if Java-side validation is correct");
        return nullptr;
    }

    /**
     * 检查某个 functionClass 是否有 native 实现。
     */
    static bool isSupported(const std::string& functionClass)
    {
        auto& registry = getRegistry();
        return registry.find(functionClass) != registry.end();
    }

private:
    static std::unordered_map<std::string, Creator>& getRegistry()
    {
        static std::unordered_map<std::string, Creator> registry = {
            // ===== 在这里注册新的 native TableFunction =====
            {"jsontest", // TODO 应该注册functionName还是functionClass
             []() { return std::make_unique<JsonSplitFunction>(); }},
            {"nrejsontest", []() { return std::make_unique<JsonSplitFunction>(); }}
            // 后续新增示例：
            // {
            //     "com.example.udf.ExplodeFunction",
            //     []() { return std::make_unique<ExplodeFunction>(); }
            // },
        };
        return registry;
    }
};

#endif // OMNISTREAM_NATIVETABLEFUNCTIONFACTORY_H
