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

#include <string>
#include "../type/StringValue.h"
#include "TupleSerializer.h"
#include "SerializerJsonInfo.h"

Tuple2Serializer::Tuple2Serializer()
{
    reuseBuffer = new Tuple2();
}

Tuple2Serializer::Tuple2Serializer(nlohmann::json& type) : Tuple2Serializer()
{
    createSerializer(type);
}

Tuple2Serializer::Tuple2Serializer(const std::vector<TypeInformation*>& types) : Tuple2Serializer()
{
    arity = types.size();
    fieldSerializers.resize(arity);
    for (int i = 0; i < arity; ++i) {
        fieldSerializers[i] = types[i]->createTypeSerializer();
    }
}

void* Tuple2Serializer::deserialize(DataInputView& source)
{
    NOT_IMPL_EXCEPTION;
}

void Tuple2Serializer::serialize(void* record, DataOutputSerializer& target)
{
    NOT_IMPL_EXCEPTION;
}

void Tuple2Serializer::deserialize(Object* buffer, DataInputView& source)
{
    LOG("TupleSerializer->deserialize start+++");
    auto tupInfo = reinterpret_cast<Tuple2*>(buffer);
    Object* buffer0 = fieldSerializers[0]->GetBuffer();
    Object* buffer1 = fieldSerializers[1]->GetBuffer();
    fieldSerializers[0]->deserialize(buffer0, source);
    fieldSerializers[1]->deserialize(buffer1, source);
    tupInfo->SetF0(buffer0);
    tupInfo->SetF1(buffer1);
    LOG("TupleSerializer->deserialize end+++");
};

void Tuple2Serializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    LOG("TupleSerializer->serialize start+++");
    auto tupInfo = reinterpret_cast<Tuple2*>(buffer);
    fieldSerializers[0]->serialize(tupInfo->f0, target);
    fieldSerializers[1]->serialize(tupInfo->f1, target);
    LOG("TupleSerializer->serialize end+++");
}

void Tuple2Serializer::createSerializer(nlohmann::json& type)
{
    arity = type.size();
    for (const auto& element : type) {
        // Access and process properties of each element
        std::string typeName;
        if (element.is_array()) {
            LOG("Tuple type is array.");
            typeName = element[1];
        } else {
            LOG("Tuple type is object.");
            typeName = element.value("type", "test");
        }
        LOG("Tuple type Name: " + typeName);
        if (typeName == "Long") {
            LOG("TupleSerializer: Long");
            auto longSerializer = new LongSerializer();
            fieldSerializers.push_back(longSerializer);
        } else if (typeName == "String") {
            LOG("TupleSerializer: String");
            auto stringSerializer = new StringSerializer();
            fieldSerializers.push_back(stringSerializer);
        } else {
            // throw OmniException("not support serializer type");
        }
    }
}

BackendDataType Tuple2Serializer::getBackendId() const
{
    return BackendDataType::TUPLE_OBJ_OBJ_BK;
}

void Tuple2Serializer::setSubBufferReusable(bool bufferReusable_)
{
    for (auto& fieldSerializer : fieldSerializers) {
        fieldSerializer->setSelfBufferReusable(bufferReusable_);
    }
}

Object* Tuple2Serializer::GetBuffer()
{
    if (bufferReusable) {
        // existed element will be replaced in set function
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new Tuple2();
}

std::string Tuple2Serializer::toJson()
{
    // 输出格式必须与 OmniAdaptor 对齐：
    //   - OmniStateSerializerHelper.convert() 解析 fieldSerializers
    //   - OmniParseFactory.buildTypeInformationBy(TUPLE) 重建字段类型
    // 每个 fieldSerializer 递归调用 toJson()，以 List<String> 形式存放内嵌 JSON 字符串；
    // 这与 OmniSerializerJsonInfo 现有 keySerializer/valueSerializer 的存储约定一致。
    nlohmann::json jsonObj;
    jsonObj["type"] = static_cast<int>(SerializerType::TUPLE);
    // element_type: Java 端用作 Class.forName 还原 TupleN 的 raw class
    jsonObj["element_type"] = std::string("org.apache.flink.api.java.tuple.Tuple") + std::to_string(arity);
    nlohmann::json fieldArr = nlohmann::json::array();
    for (auto* fs : fieldSerializers) {
        fieldArr.push_back(fs ? fs->toJson() : std::string());
    }
    jsonObj["fieldSerializers"] = fieldArr;
    return jsonObj.dump();
}
