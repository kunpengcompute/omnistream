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

#ifndef OMNISTREAM_BASICTYPEINFO_H
#define OMNISTREAM_BASICTYPEINFO_H
#include "TypeInformation.h"
#include "typeconstants.h"
#include "core/typeutils/StringSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/BigIntSerializer.h"
#include "core/typeutils/DoubleSerializer.h"
#include "typeutils/VoidSerializer.h"

class BasicTypeInfo : public TypeInformation {
public:
    BasicTypeInfo(TypeSerializer* serializer, std::string typeName) : serializer(serializer), typeName(typeName)
    {
    }

    explicit BasicTypeInfo(std::string typeName) : typeName(typeName)
    {
    }

    TypeSerializer* createTypeSerializer() override
    {
        if (typeName == TYPE_NAME_STRING_SERIALIZER) {
            serializer = new StringSerializer();
        } else if (typeName == TYPE_NAME_BIGINT_SERIALIZER) {
            serializer = new BigIntSerializer();
        } else if (typeName == TYPE_NAME_LONG_SERIALIZER) {
            serializer = new LongSerializer();
        } else if (typeName == TYPE_NAME_DOUBLE_SERIALIZER) {
            serializer = new DoubleSerializer();
        } else if (typeName == TYPE_NAME_INT_SERIALIZER) {
            serializer = new IntSerializer();
        } else if (typeName == TYPE_NAME_VOID_SERIALIZER) {
            serializer = new VoidSerializer();
        }
        return serializer;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }
    std::string name() override
    {
        return typeName;
    }
    static BasicTypeInfo* getBasicTypeInfo(std::string typeName_);

    static BasicTypeInfo* getBasicTypeInfoByClass(std::string className);

    // todo: need add other type
    // Typeinfo is Object.Multithreading can easily lead to reference technology exceptions. need thread_local.
    thread_local static BasicTypeInfo* STRING_TYPE_INFO;
    thread_local static BasicTypeInfo* BIG_INT_TYPE_INFO;
    thread_local static BasicTypeInfo* LONG_TYPE_INFO;
    thread_local static BasicTypeInfo* INT_TYPE_INFO;
    thread_local static BasicTypeInfo* DOUBLE_TYPE_INFO;

private:
    TypeSerializer* serializer;
    const std::string typeName;
};

#endif // OMNISTREAM_BASICTYPEINFO_H
