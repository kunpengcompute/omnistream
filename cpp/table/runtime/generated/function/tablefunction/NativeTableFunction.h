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

#ifndef OMNISTREAM_NATIVETABLEFUNCTION_H
#define OMNISTREAM_NATIVETABLEFUNCTION_H

#include <string>
#include <vector>

/**
 * Native C++ 实现的 TableFunction 抽象基类。
 *
 * 对标 Java 侧的 org.apache.flink.table.functions.TableFunction，
 * 但只处理列式数据：输入一个 string，输出 0~N 个 string。
 *
 * 后续扩展时可以增加更多 eval 重载（如多参数、不同类型等）。
 */
class NativeTableFunction {
public:
    virtual ~NativeTableFunction() = default;

    /**
     * 核心 eval 方法：输入一个字符串参数，返回 0~N 个结果字符串。
     * 空 vector 表示该行无输出（用于 LEFT JOIN 时填 null）。
     *
     * @param input 输入字符串（可能为空字符串或无效值）
     * @return 输出的字符串列表
     */
    virtual std::vector<std::string> eval(const std::string& input) = 0;

    /**
     * 返回函数名称，用于日志和调试。
     */
    virtual std::string name() const = 0;
};

#endif // OMNISTREAM_NATIVETABLEFUNCTION_H
