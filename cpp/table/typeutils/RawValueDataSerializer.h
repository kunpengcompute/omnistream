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

#pragma once

#include <stdexcept>
#include <string>
#include <utility>

#include "core/typeutils/SerializerJsonInfo.h"
#include "core/typeutils/TypeSerializerSingleton.h"

// Metadata-only serializer for Flink RawValueData accumulator placeholders.
// GroupAgg stores the actual DataView contents in separate keyed states, so a
// RAW field in accState must always be null and has no value payload to encode.
class RawValueDataSerializer : public TypeSerializerSingleton {
public:
    RawValueDataSerializer(std::string serializerInstanceClazz, std::string serializerSnapshot)
        : serializerInstanceClazz_(std::move(serializerInstanceClazz)),
          serializerSnapshot_(std::move(serializerSnapshot))
    {
    }

    void* deserialize(DataInputView&) override
    {
        ERROR_RELEASE("The deserialize is null.");
        throw std::runtime_error("RawValueDataSerializer cannot deserialize non-null RAW accumulator values");
    }

    void serialize(void*, DataOutputSerializer&) override
    {
        ERROR_RELEASE("The serialize is null.");
        throw std::runtime_error("RawValueDataSerializer cannot serialize non-null RAW accumulator values");
    }

    const char* getName() const override
    {
        return "RawValueDataSerializer";
    }

    std::string toJson() override
    {
        return "";
    }

private:
    std::string serializerInstanceClazz_;
    std::string serializerSnapshot_;
};
