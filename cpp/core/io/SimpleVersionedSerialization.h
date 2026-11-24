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

#ifndef OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H
#define OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H

#include <vector>
#include <stdexcept>
#include <memory>

#include "core/include/common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/io/SimpleVersionedSerializer.h"

#include "SimpleVersionedSerializer.h"

class SimpleVersionedSerialization {
public:
    template <typename T>
    static void writeVersionAndSerialize(
        SimpleVersionedSerializer<T>& serializer, const T& obj, DataOutputSerializer& out)
    {
        std::vector<uint8_t> serialized = serializer.serialize(obj);
        out.writeInt(serializer.getVersion());
        out.writeInt(serialized.size());
        out.write(serialized.data(), serialized.size(), 0, serialized.size());
    }

    template <typename T>
    static void writeVersionAndSerializeList(
        SimpleVersionedSerializer<T>& serializer, const std::vector<T>& objList, DataOutputSerializer& out)
    {
        out.writeInt(serializer.getVersion());
        out.writeInt(objList.size());
        for (const T& obj : objList) {
            std::vector<uint8_t> serialized = serializer.serialize(obj);
            out.writeInt(serialized.size());
            out.write(serialized.data(), serialized.size(), 0, serialized.size());
        }
    }

    // 写入版本号并序列化对象
    template <typename T>
    static std::vector<uint8_t> writeVersionAndSerialize(SimpleVersionedSerializer<T>& serializer, const T& obj)
    {
        // 简化实现：先写入版本号（4字节），然后写入序列化数据
        std::vector<uint8_t> result;
        int version = serializer.getVersion();

        // 写入版本号（大端序）
        result.push_back((version >> 24) & 0xFF);
        result.push_back((version >> 16) & 0xFF);
        result.push_back((version >> 8) & 0xFF);
        result.push_back(version & 0xFF);

        // 写入序列化数据
        std::vector<uint8_t> serialized = serializer.serialize(obj);
        int length = serialized.size();
        // 写入序列化数据长度（大端序）
        result.push_back((length >> 24) & 0xFF);
        result.push_back((length >> 16) & 0xFF);
        result.push_back((length >> 8) & 0xFF);
        result.push_back(length & 0xFF);

        result.insert(result.end(), serialized.begin(), serialized.end());

        return result;
    }

    // 读取版本号并反序列化对象
    template <typename T>
    static T* readVersionAndDeSerialize(SimpleVersionedSerializer<T>& serializer, std::vector<uint8_t>& bytes)
    {
        // 简化实现：先读取版本号（4字节），然后反序列化数据
        if (bytes.size() < 8) {
            INFO_RELEASE("Exception: Insufficient data for version deserialization, size=" << bytes.size());
            throw std::runtime_error("Insufficient data for version deserialization");
        }

        // 读取版本号（大端序）
        int version = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];

        int length = (bytes[4] << 24) | (bytes[5] << 16) | (bytes[6] << 8) | bytes[7];

        // 提取序列化数据部分
        std::vector<uint8_t> serializedData(bytes.begin() + 8, bytes.end());

        // 反序列化对象
        return serializer.deserialize(version, serializedData);
    }

    template <typename T>
    static T* readVersionAndDeSerialize(SimpleVersionedSerializer<T>& serializer, DataInputDeserializer& input)
    {
        // 读取版本号
        int version = input.readInt();

        // 读取列表大小
        int dataSize = input.readInt();

        std::vector<uint8_t> data(dataSize);

        input.readFully(data.data(), dataSize, 0, dataSize);

        // 反序列化对象
        return serializer.deserialize(version, data);
    }

    // 读取版本号并反序列化列表 (使用DataInputView)
    template <typename T>
    static std::vector<T>* readVersionAndDeserializeList(
        SimpleVersionedSerializer<T>& serializer, DataInputDeserializer& in)
    {
        // 读取版本号
        int serializerVersion = in.readInt();

        // 读取列表大小
        int dataSize = in.readInt();

        std::vector<T>* data = new std::vector<T>();
        data->reserve(dataSize);

        for (int ignored = 0; ignored < dataSize; ++ignored) {
            // 读取元素大小
            int datumSize = in.readInt();

            // 读取元素数据
            std::vector<uint8_t> datum(datumSize);
            in.readFully(datum.data(), datumSize, 0, datumSize);

            // 反序列化元素
            T* element = serializer.deserialize(serializerVersion, datum);
            data->push_back(std::move(*element));
        }
        return data;
    }
};

#endif // OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H
