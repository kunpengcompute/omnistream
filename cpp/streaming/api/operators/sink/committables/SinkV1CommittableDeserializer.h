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

#ifndef OMNISTREAM_SINKV1COMMITTABLEDESERIALIZER_H
#define OMNISTREAM_SINKV1COMMITTABLEDESERIALIZER_H

#include "core/memory/DataInputDeserializer.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/io/SimpleVersionedSerialization.h"

class SinkV1CommittableDeserializer {
public:
    static constexpr int MAGIC_NUMBER = 0xb91f252c;
    template <typename T>
    static std::vector<T>* readVersionAndDeserializeList(
        std::shared_ptr<SimpleVersionedSerializer<T>>& serializer, DataInputDeserializer& in)
    {
        validateMagicNumber(in);
        return SimpleVersionedSerialization::readVersionAndDeserializeList<T>(*serializer, in);
    }

private:
    static void validateMagicNumber(DataInputDeserializer& in)
    {
        const int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw std::runtime_error("Corrupt data: Unexpected magic number");
        }
    }
};

#endif // OMNISTREAM_SINKV1COMMITTABLEDESERIALIZER_H
