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

#include "DeserializationFactory.h"
#include "JsonDeserializationSchema.h"
#include "JsonRowDataDeserializationSchema.h"
#include "SimpleStringSchema.h"

#include <stdexcept>

DeserializationSchema* DeserializationFactory::getDeserializationSchema(nlohmann::json& opDescriptionJSON)
{
    auto& schemaName = opDescriptionJSON["deserializationSchema"];
    if (schemaName == "JsonDeserializationSchema") {
        return new JsonDeserializationSchema("json");
    }
    if (schemaName == "SimpleStringSchema") {
        return new SimpleStringSchema();
    }
    if (schemaName == "JsonRowDataDeserializationSchema") {
        return new JsonRowDataDeserializationSchema(opDescriptionJSON);
    }

    throw std::runtime_error("Unsupported deserialization schema: " + schemaName.dump());
}
