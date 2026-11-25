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

#include "BigIntSerializer.h"

void BigIntSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    writeBigInteger(buffer, target);
}

void BigIntSerializer::deserialize(Object* buffer, DataInputView& source)
{
    readBigInteger(buffer, source);
}

void BigIntSerializer::readBigInteger(Object* buffer, DataInputView& source)
{
    auto bigInteger = static_cast<BigInteger*>(buffer);
    int len = source.readInt();
    if (len < 4) {
        switch (len) {
            case 0: return;
            case 1: *bigInteger = ZERO; return;
            case 2: *bigInteger = ONE; return;
            case 3: *bigInteger = TEN; return;
            default: THROW_RUNTIME_ERROR("BigIntSerializer::readBigInteger len is error");
        }
    }

    int size = len - 4;
    std::vector<uint8_t> bytes;
    bytes.resize(size);
    source.readFully(bytes.data(), size, 0, size);
    bigInteger->setByteArray(bytes.data(), size, 0, size);
}

void BigIntSerializer::writeBigInteger(Object* buffer, DataOutputSerializer& target)
{
    auto bigInteger = static_cast<BigInteger*>(buffer);
    // fast paths for 0, 1, 10
    // only reference equality is checked because equals would be too expensive
    if (buffer == nullptr) {
        target.writeInt(0);
        return;
    } else if (*bigInteger == ZERO) {
        target.writeInt(1);
        return;
    } else if (*bigInteger == ONE) {
        target.writeInt(2);
        return;
    } else if (*bigInteger == TEN) {
        target.writeInt(3);
        return;
    }
    // default
    auto bytes = bigInteger->toByteArray();
    // the length we write is offset by four, because null and short-paths for ZERO, ONE, and
    // TEN
    target.writeInt(bytes.size() + 4);
    target.write(bytes.data(), bytes.size(), 0, bytes.size());
}

BigIntSerializer* BigIntSerializer::INSTANCE = new BigIntSerializer();

Object* BigIntSerializer::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new BigInteger();
}
