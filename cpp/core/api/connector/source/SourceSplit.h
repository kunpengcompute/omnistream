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

#ifndef FLINK_TNEL_SOURCESPLIT_H
#define FLINK_TNEL_SOURCESPLIT_H

#include <string>

// 定义抽象基类 SourceSplit
class SourceSplit {
public:
    // 纯虚函数，类似于 Java 接口中的抽象方法
    virtual std::string splitId() const = 0;

    // 虚析构函数，确保派生类对象能被正确释放
    virtual ~SourceSplit() = default;
};

#endif // FLINK_TNEL_SOURCESPLIT_H
