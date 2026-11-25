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

#ifndef OMNISTREAM_SERIALIZERJSONINFO_H
#define OMNISTREAM_SERIALIZERJSONINFO_H

#include <optional>

#include "table/types/logical/LogicalType.h"

#include "TypeSerializer.h"

enum class SerializerType {
    UNKNOWN = 0,
    LIST = 1,            /* use fields [type, valueSerializer] */
    BIG_INT = 2,         /* use fields [type] */
    LONG = 3,            /* use fields [type] */
    INT = 4,             /* use fields [type] */
    DOUBLE = 5,          /* use fields [type] */
    MAP = 6,             /* use fields [type, keySerializer, valueSerializer] */
    POJO = 7,            /* use fields [type, fieldNames, fieldSerializers] */
    STRING_T = 8,        /* use fields [type] */
    BOOLEAN = 9,         /* use fields [type] */
    VOID = 10,           /* use fields [type] */
    VOID_NAMESPACE = 11, /* use fields [type] */
    TIMER = 12,          /* use fields [type, keySerializer, namespaceSerializer] */
    // TUPLE = 13 与 OmniAdaptor Java 端 OmniSerializerType.TUPLE 的 code 必须保持一致
    TUPLE = 13,                /* use fields [type, elementType, fieldSerializers] */
    BYTE_PRIMITIVE_ARRAY = 14, /* use fields [type, valueSerializer] */
    ROW = 15,                  /* use fields [type, logicalType] */
    BINARY_ROW = 16,           /* use fields [type, fieldNames] */
    EXTERNAL = 17,             /* use fields [type, logicalType, valueSerializer, serializerAttributes] */
    TIME_WINDOW = 18,          /* use fields [type, elementType] */
};

struct SerializerAttributes {
    // SerializerAttributes JSON 使用的字段 key。
    static constexpr const char* EXTERNAL_IS_INTERNAL_INPUT_KEY = "externalIsInternalInput";
    static constexpr const char* DATA_TYPE_CONVERSION_CLASS_NAME_KEY = "dataTypeConversionClassName";

    std::optional<bool> externalIsInternalInput;
    std::string dataTypeConversionClassName = "";

    nlohmann::json toJson() const
    {
        nlohmann::json jsonObj;
        if (externalIsInternalInput.has_value()) {
            jsonObj[EXTERNAL_IS_INTERNAL_INPUT_KEY] = externalIsInternalInput.value();
        }
        jsonObj[DATA_TYPE_CONVERSION_CLASS_NAME_KEY] = dataTypeConversionClassName;

        return jsonObj;
    }
};

struct SerializerJsonInfo {
    // C++ serializer JSON / JNI metadata 协议使用的字段 key。
    // StateMetaInfoSnapshot 内部使用 Flink canonical CommonSerializerKeys。
    static constexpr const char* TYPE_KEY = "type";
    static constexpr const char* ELEMENT_TYPE_KEY = "element_type";
    static constexpr const char* KEY_SERIALIZER_KEY = "keySerializer";
    static constexpr const char* NAMESPACE_SERIALIZER_KEY = "namespaceSerializer";
    static constexpr const char* VALUE_SERIALIZER_KEY = "valueSerializer";
    static constexpr const char* STATE_SERIALIZER_KEY = "stateSerializer";
    static constexpr const char* LOGICAL_TYPE_KEY = "logicalType";
    static constexpr const char* FIELDS_KEY = "fields";
    static constexpr const char* FIELD_NAME_KEY = "fieldName";
    static constexpr const char* FIELD_SERIALIZER_KEY = "fieldSerializer";
    static constexpr const char* SERIALIZER_ATTRIBUTES_KEY = "serializerAttributes";

    // 基础类型及string只用type字段
    SerializerType type = SerializerType::UNKNOWN;
    // elementType pojo使用，类的全路径限定名 com.example.xxx
    std::string elementType = "";
    // keySerializer只有map使用
    TypeSerializer* keySerializer;
    // valueSerializer map及list中的pojo使用
    TypeSerializer* valueSerializer;
    // namespaceSerializer TimerSerializer 使用
    TypeSerializer* namespaceSerializer;
    // fieldSerializers和fieldNames pojo使用
    std::vector<TypeSerializer*> fieldSerializers;
    std::vector<std::string> fieldNames;
    LogicalType* logicalType;
    SerializerAttributes* serializerAttributes;

public:
    std::string toJson()
    {
        nlohmann::json jsonObj;
        jsonObj[TYPE_KEY] = type;
        jsonObj[ELEMENT_TYPE_KEY] = elementType;
        if (keySerializer != nullptr) {
            jsonObj[KEY_SERIALIZER_KEY] = keySerializer->toJson();
        }
        if (valueSerializer != nullptr) {
            jsonObj[VALUE_SERIALIZER_KEY] = valueSerializer->toJson();
        }
        if (namespaceSerializer != nullptr) {
            jsonObj[NAMESPACE_SERIALIZER_KEY] = namespaceSerializer->toJson();
        }
        if (logicalType != nullptr) {
            jsonObj[LOGICAL_TYPE_KEY] = logicalType->toJson();
        }
        if (serializerAttributes != nullptr) {
            jsonObj[SERIALIZER_ATTRIBUTES_KEY] = serializerAttributes->toJson();
        }
        if (fieldSerializers.size() != fieldNames.size()) {
            return jsonObj.dump();
        }
        if (fieldSerializers.size() == 0) {
            return jsonObj.dump();
        }

        nlohmann::json fieldTypesJson = nlohmann::json::array();
        for (auto i = 0; i < fieldSerializers.size(); i++) {
            nlohmann::json fieldJson;
            auto fieldName = fieldNames[i];
            auto fieldSerializer = fieldSerializers[i];
            if (fieldSerializer == nullptr) {
                continue;
            }
            fieldJson[FIELD_NAME_KEY] = fieldName;
            fieldJson[FIELD_SERIALIZER_KEY] = fieldSerializer->toJson();
            fieldTypesJson.push_back(std::move(fieldJson));
        }
        jsonObj[FIELDS_KEY] = fieldTypesJson.dump();
        return jsonObj.dump();
    };
};

#endif // OMNISTREAM_SERIALIZERJSONINFO_H
