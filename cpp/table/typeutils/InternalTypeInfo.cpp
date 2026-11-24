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

#include "InternalTypeInfo.h"
#include "InternalSerializers.h"

InternalTypeInfo::InternalTypeInfo(LogicalType* type, TypeSerializer* typeSerializer)
    : type_(type),
      typeSerializer(typeSerializer)
{
}

TypeSerializer* InternalTypeInfo::createTypeSerializer()
{
    return typeSerializer;
}

InternalTypeInfo::~InternalTypeInfo() = default;

InternalTypeInfo* InternalTypeInfo::of(LogicalType* type)
{
    auto serializer = InternalSerializers::create(type);
    return new InternalTypeInfo(type, serializer);
}

InternalTypeInfo* InternalTypeInfo::ofRowType(omnistream::RowType* type)
{
    return of(dynamic_cast<LogicalType*>(type));
}

std::string InternalTypeInfo::name()
{
    std::stringstream ss;
    ss << "InternalType: type id" << type_->getTypeId();
    ss << "typeSerializer: " << typeSerializer->getName();
    return ss.str();
}

InternalTypeInfo* InternalTypeInfo::INT_TYPE = InternalTypeInfo::of(BasicLogicalType::INTEGER);
