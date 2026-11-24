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

#include "TupleTypeInfo.h"
#include "core/typeutils/TupleSerializer.h"

std::string TupleTypeInfo::name()
{
    std::stringstream ss;
    ss << "TupleTypeInfo: type id" << "tes";
    ss << "typeSerializer: " << typeSerializer->getName();
    return ss.str();
}

TypeSerializer* TupleTypeInfo::createTypeSerializer()
{
    typeSerializer = new Tuple2Serializer(types);
    return typeSerializer;
}

TupleTypeInfo* TupleTypeInfo::of(const json& type)
{
    auto* serializer = new Tuple2Serializer(const_cast<json&>(type));
    serializer->setSelfBufferReusable(true);
    return new TupleTypeInfo(serializer);
}

BackendDataType TupleTypeInfo::getBackendId() const
{
    return typeSerializer->getBackendId();
}

TypeSerializer* TupleTypeInfo::getTypeSerializer()
{
    // TupleTypeInfo 有两种构造路径：
    //   1) of(json)  → 在构造时直接传入构造好的 Tuple2Serializer，typeSerializer 已设置
    //   2) (vector<TypeInformation*>) → 仅记录字段类型，typeSerializer 为 null，需要 lazy init。
    // restore 路径走的是 (2)（createDataStreamTypeInfo 解析 fieldSerializers 后 new TupleTypeInfo(types)），
    // 这里若不 lazy 创建，HeapKeyedStateBackendBuilder 会拿到 null serializer 把 state 全跳过。
    if (typeSerializer == nullptr && !types.empty()) {
        return createTypeSerializer();
    }
    return typeSerializer;
}
