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

#ifndef OMNISTREAM_BYTEPRIMITIVEARRAYTYPEINFO_H
#define OMNISTREAM_BYTEPRIMITIVEARRAYTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "../typeutils/BytePrimitiveArraySerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"

class BytePrimitiveArrayTypeInfo : public TypeInformation {
public:
    TypeSerializer* createTypeSerializer() override
    {
        return new BytePrimitiveArraySerializer(nullptr);
    };

    std::string name() override
    {
        return name_;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BYTE_ARRAY_BK;
    };

private:
    const char* name_ = TYPE_NAME_BYTE_PRIMITIVE_ARRAY_SERIALIZER;
};

#endif // OMNISTREAM_BYTEPRIMITIVEARRAYTYPEINFO_H
