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

#ifndef OMNISTREAM_JOINTUPLE_H
#define OMNISTREAM_JOINTUPLE_H

#include "TypeInformation.h"
#include "typeconstants.h"

#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/JoinTupleSerializer2.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "table/typeutils/SortedVectorLong.h"

class CustomTypeInfo : public TypeInformation {
public:
    CustomTypeInfo(std::string className)
    {
        if (className == TYPE_NAME_JOIN_TUPLE_CLASS || className == TYPE_NAME_JOIN_TUPLE_CLASS_LINE) {
            typeSerializer_ = new JoinTupleSerializer();
        } else if (className == TYPE_NAME_JOIN_TUPLE2_CLASS || className == TYPE_NAME_JOIN_TUPLE2_CLASS_LINE) {
            typeSerializer_ = new JoinTupleSerializer2();
        } else if (className == TYPE_NAME_XXH128_HASH_CLASS || className == TYPE_NAME_XXH128_HASH_CLASS_LINE) {
            typeSerializer_ = new XxH128_hashSerializer();
        } else if (className == TYPE_NAME_VECTOR_BATCH_CLASS || className == TYPE_NAME_VECTOR_BATCH_CLASS_LINE) {
            typeSerializer_ = new VectorBatchSerializer();
        } else if (
            className == TYPE_NAME_SORTED_VECTOR_LONG_CLASS || className == TYPE_NAME_SORTED_VECTOR_LONG_CLASS_LINE) {
            typeSerializer_ = new SortedVectorLong();
        } else {
            throw new std::runtime_error("unknown className : " + className);
        }
    }

    std::string name() override
    {
        return typeSerializer_->getName();
    }

    BackendDataType getBackendId() const override
    {
        return typeSerializer_->getBackendId();
    }

    TypeSerializer* createTypeSerializer() override
    {
        return typeSerializer_;
    }

    static bool isCustomType(std::string className)
    {
        if (className == TYPE_NAME_JOIN_TUPLE_CLASS || className == TYPE_NAME_JOIN_TUPLE_CLASS_LINE ||
            className == TYPE_NAME_JOIN_TUPLE2_CLASS || className == TYPE_NAME_JOIN_TUPLE2_CLASS_LINE ||
            className == TYPE_NAME_XXH128_HASH_CLASS || className == TYPE_NAME_XXH128_HASH_CLASS_LINE ||
            className == TYPE_NAME_VECTOR_BATCH_CLASS || className == TYPE_NAME_VECTOR_BATCH_CLASS_LINE ||
            className == TYPE_NAME_SORTED_VECTOR_LONG_CLASS || className == TYPE_NAME_SORTED_VECTOR_LONG_CLASS_LINE) {
            return true;
        }
        return false;
    }

    static CustomTypeInfo* build(std::string className)
    {
        return new CustomTypeInfo(className);
    }

private:
    TypeSerializer* typeSerializer_;
};
#endif // OMNISTREAM_JOINTUPLE_H
