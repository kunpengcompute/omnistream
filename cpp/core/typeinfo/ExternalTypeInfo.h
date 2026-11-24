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

#ifndef OMNISTREAM_EXTERNALTYPEINFO_H
#define OMNISTREAM_EXTERNALTYPEINFO_H

#include <memory>

#include "TypeInformation.h"
#include "table/types/logical/LogicalType.h"

class ExternalTypeInfo : public TypeInformation {
public:
    ExternalTypeInfo(
        std::shared_ptr<LogicalType> dataType,
        TypeInformation* internalTypeInfo,
        bool isInternalInput,
        std::string conversionClass);

    ~ExternalTypeInfo() override;

    TypeSerializer* createTypeSerializer() override;

    BackendDataType getBackendId() const override;

    std::string name() override;

    LogicalType* getDataType() const;

    TypeInformation* getInternalTypeInfo() const;

    bool isInternalInput() const;

    const std::string& getConversionClass() const;

private:
    std::shared_ptr<LogicalType> dataType_;
    TypeInformation* internalTypeInfo_;
    bool isInternalInput_;
    std::string conversionClass_;
};

#endif // OMNISTREAM_EXTERNALTYPEINFO_H
