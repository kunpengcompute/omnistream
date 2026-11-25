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

#include <utility>

#include "ExternalTypeInfo.h"

#include "core/typeutils/ExternalSerializer.h"

ExternalTypeInfo::ExternalTypeInfo(
    std::shared_ptr<LogicalType> dataType,
    TypeInformation* internalTypeInfo,
    bool isInternalInput,
    std::string conversionClass)
    : dataType_(std::move(dataType)),
      internalTypeInfo_(internalTypeInfo),
      isInternalInput_(isInternalInput),
      conversionClass_(std::move(conversionClass))
{
}

ExternalTypeInfo::~ExternalTypeInfo()
{
    if (internalTypeInfo_ != nullptr) {
        internalTypeInfo_->putRefCount();
        internalTypeInfo_ = nullptr;
    }
}

TypeSerializer* ExternalTypeInfo::createTypeSerializer()
{
    auto internalSerializer = std::unique_ptr<TypeSerializer>(internalTypeInfo_->createTypeSerializer());
    auto serializer = new ExternalSerializer(dataType_.get(), internalSerializer.get(), isInternalInput_);
    internalSerializer.release();
    return serializer;
}

BackendDataType ExternalTypeInfo::getBackendId() const
{
    if (dataType_->getTypeId() == omniruntime::type::DataTypeId::OMNI_LONG) {
        return BackendDataType::EXTERNAL_BIGINT_BK;
    }
    return BackendDataType::INVALID_BK;
}

std::string ExternalTypeInfo::name()
{
    return "ExternalTypeInfo<" + conversionClass_ + ">";
}

LogicalType* ExternalTypeInfo::getDataType() const
{
    return dataType_.get();
}

TypeInformation* ExternalTypeInfo::getInternalTypeInfo() const
{
    return internalTypeInfo_;
}

bool ExternalTypeInfo::isInternalInput() const
{
    return isInternalInput_;
}

const std::string& ExternalTypeInfo::getConversionClass() const
{
    return conversionClass_;
}
