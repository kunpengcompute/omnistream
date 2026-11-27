/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <cstdint>
#include <stdexcept>
#include "StringValue.h"

StringValue::StringValue() = default;

void StringValue::write(DataOutputSerializer& out)
{
    unsigned int len = len_;

    // write the length, variable-length encoded
    while (len >= HIGH_BIT) {
        out.write(len | HIGH_BIT);
        len >>= 7;
    }
    out.write(len);

    // write the char data, variable length encoded
    for (unsigned int i = 0; i < len_; i++) {
        auto c = static_cast<uint32_t>(value_[i]);

        while (c >= HIGH_BIT) {
            out.write(c | HIGH_BIT);
            c >>= 7;
        }
        out.write(c);
    }
}

void StringValue::read(DataInputView& in)
{
    value_.clear();

    unsigned int len = in.readUnsignedByte();
    if (len >= HIGH_BIT) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }
    len_ = len;

    value_.reserve(len);

    // len is the size of char of string not the byte size of the string
    for (unsigned int i = 0; i < len; i++) {
        unsigned int c = in.readUnsignedByte();
        if (c < HIGH_BIT) {
            value_.push_back(c);
        } else {
            int shift = 7;
            unsigned int curr;
            c = c & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                c |= (curr & 0x7f) << shift;
                shift += 7;
            }
            c |= curr << shift;
            value_.push_back(c);
        }
    }
}

// return the ref, means the value_ ownership is still in this object, although caller may change the internal
// content of value_
const std::u32string& StringValue::getValue() const
{
    return value_;
}

// notice, the input argument value is copied (Copy Assignment Operator) to the internal value_
// value is copied
void StringValue::setValue(const std::u32string& value)
{
    value_ = value;
    len_ = value.size();
}

void StringValue::writeString(const std::u32string* value, DataOutputSerializer& out)
{
    if (value != nullptr) {
        auto& cs = *value;

        uint32_t strlen = cs.length();

        // the length we write is offset_ by one, because a length of zero indicates a null value
        uint32_t lenToWrite = strlen + 1;
        if (lenToWrite < strlen) {
            THROW_LOGIC_EXCEPTION("CharSequence is too long.");
        }

        // string is prefixed by it's variable length encoded size, which can take 1-5 bytes.
        if (lenToWrite < HIGH_BIT) {
            out.write(static_cast<uint8_t>(lenToWrite));
        } else if (lenToWrite < HIGH_BIT14) {
            out.write((lenToWrite | HIGH_BIT));
            out.write((lenToWrite >> 7));
        } else if (lenToWrite < HIGH_BIT21) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14));
        } else if (lenToWrite < HIGH_BIT28) {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21));
        } else {
            out.write(lenToWrite | HIGH_BIT);
            out.write((lenToWrite >> 7) | HIGH_BIT);
            out.write((lenToWrite >> 14) | HIGH_BIT);
            out.write((lenToWrite >> 21) | HIGH_BIT);
            out.write((lenToWrite >> 28));
        }
        // write the char data, variable length encoded
        for (uint32_t i = 0; i < strlen; i++) {
            uint32_t c = cs[i];

            // manual loop unroll, as it performs much better on jdk8
            if (c < HIGH_BIT) {
                out.write(c);
            } else if (c < HIGH_BIT14) {
                out.write(c | HIGH_BIT);
                out.write((c >> 7));
            } else {
                out.write(c | HIGH_BIT);
                out.write((c >> 7) | HIGH_BIT);
                out.write((c >> 14));
            }
        }
    } else {
        out.write(0);
    }
}

std::u32string* StringValue::readString(SysDataInput& in)
{
    // the length we read is offset_ by one, because a length of zero indicates a null value
    auto data_ = in.getData();
    auto position_ = in.getPosition();
    unsigned int len = static_cast<int>(data_[position_++]);

    LOG("first len" + std::to_string(len));

    if (len == 0) {
        in.setPosition(position_);
        return nullptr;
    }

    if (len >= HIGH_BIT) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = static_cast<int>(data_[position_++])) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }

    // subtract one for the null length
    len -= 1;
    LOG("final len" + std::to_string(len));

    auto* data = new std::u32string();
    if (len > SHORT_STRING_MAX_LENGTH) {
        data->reserve(len);
    } else {
        data->reserve(SHORT_STRING_MAX_LENGTH);
    }

    for (unsigned int i = 0; i < len; i++) {
        unsigned int c = static_cast<int>(data_[position_++]);
        if (c >= HIGH_BIT) {
            int shift = 7;
            unsigned int curr;
            c = c & 0x7f;
            while ((curr = static_cast<int>(data_[position_++])) >= HIGH_BIT) {
                c |= (curr & 0x7f) << shift;
                shift += 7;
            }
            c |= curr << shift;
        }
        data->push_back(c);
    }

    LOG("final string" + u32string_to_std_string(*data));
    in.setPosition(position_);
    return data;
}

void StringValue::writeString(String* buffer, DataOutputSerializer& out)
{
    std::string_view value = buffer->getValue();
    if (likely(!value.empty())) {
        uint32_t strlen = value.size();

        // the length we write is offset_ by one, because a length of zero indicates a null value
        uint32_t lenToWrite = strlen + 1;
        if (unlikely(lenToWrite < strlen)) {
            THROW_LOGIC_EXCEPTION("CharSequence is too long.");
        }

        // opt: Save the serialization result through a temporary array,
        // and then call the batch structure to write it to the output byte stream.
        int32_t maxLimit = 5 + 3 * strlen;
        uint8_t serializedData[maxLimit];
        int32_t index = 0;
        if (likely(lenToWrite < HIGH_BIT)) {
            serializedData[index++] = (lenToWrite & 0xff);
        } else if (lenToWrite < HIGH_BIT14) {
            serializedData[index++] = ((lenToWrite | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 7) & 0xff);
        } else if (lenToWrite < HIGH_BIT21) {
            serializedData[index++] = ((lenToWrite | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 14) & 0xff);
        } else if (lenToWrite < HIGH_BIT28) {
            serializedData[index++] = ((lenToWrite | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 14 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 21) & 0xff);
        } else {
            serializedData[index++] = ((lenToWrite | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 14 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 21 | HIGH_BIT) & 0xff);
            serializedData[index++] = ((lenToWrite >> 28) & 0xff);
        }

        for (uint32_t i = 0; i < strlen; i++) {
            uint32_t c = value[i];

            // manual loop unroll, as it performs much better on jdk8
            if (c < HIGH_BIT) {
                serializedData[index++] = (c & 0xff);
            } else if (c < HIGH_BIT14) {
                serializedData[index++] = ((c | HIGH_BIT) & 0xff);
                serializedData[index++] = ((c >> 7) & 0xff);
            } else {
                serializedData[index++] = ((lenToWrite | HIGH_BIT) & 0xff);
                serializedData[index++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
                serializedData[index++] = ((lenToWrite >> 14) & 0xff);
            }
        }

        // use memcpy to write all data
        out.write((uint8_t*)serializedData, maxLimit, 0, index);
    } else {
        out.write(0);
    }
}

void StringValue::readString(String* buffer, SysDataInput& in)
{
    // the length we read is offset_ by one, because a length of zero indicates a null value
    auto data_ = in.getData();
    auto position_ = in.getPosition();
    unsigned int len = static_cast<int>(data_[position_++]);
#ifdef DEBUG
    LOG("first len" + std::to_string(len));
#endif
    if (unlikely(len == 0)) {
        in.setPosition(position_);
        return;
    }

    if (unlikely(len >= HIGH_BIT)) {
        int shift = 7;
        unsigned int curr;
        len = len & 0x7f;
        while ((curr = static_cast<int>(data_[position_++])) >= HIGH_BIT) {
            len |= (curr & 0x7f) << shift;
            shift += 7;
        }
        len |= curr << shift;
    }

    // subtract one for the null length
    len -= 1;
    char* data = buffer->getData();
    size_t capacity = buffer->getSize();
    // read all date into str
    if (unlikely(capacity < len)) {
        buffer->resize(len * 2);
        data = buffer->data();
    }

    for (uint32_t i = 0; i < len; i++) {
        unsigned int c = static_cast<int>(data_[position_++]);
        if (c >= HIGH_BIT) {
            int shift = 7;
            unsigned int curr;
            c = c & 0x7f;
            while ((curr = static_cast<int>(data_[position_++])) >= HIGH_BIT) {
                c |= (curr & 0x7f) << shift;
                shift += 7;
            }
            c |= curr << shift;
        }
        data[i] = c;
    }
    buffer->resize(len);
    in.setPosition(position_);
}
