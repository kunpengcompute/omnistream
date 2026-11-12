/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "table/types/logical/LogicalType.h"
#include "core/api/common/serialization/DeserializationSchema.h"

class JsonRowDataDeserializationSchema : public DeserializationSchema {
public:
    explicit JsonRowDataDeserializationSchema(nlohmann::json& opDescriptionJSON)
        : fieldNames(opDescriptionJSON["outputNames"].get<std::vector<std::string>>()),
          ignoreParseErrors(opDescriptionJSON.value("ignoreParseErrors", false)),
          failOnMissingField(opDescriptionJSON.value("failOnMissingField", false))
    {
        auto outputTypes = opDescriptionJSON["outputTypes"].get<std::vector<std::string>>();
        for (std::string type : outputTypes) {
            fieldTypes.push_back(LogicalType::flinkTypeToOmniTypeId(type));
        }
    }

    void Open() override
    {
    }

    void deserialize(const uint8_t* message, size_t length, Collector* out) override;

    void* deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec) override;

    void deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec,
        std::vector<int64_t>& timeVec, Collector* out) override;

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    std::unique_ptr<omnistream::VectorBatch> deserializeBatch(const std::vector<const uint8_t*>& messages,
        const std::vector<size_t>& lengths, const std::vector<int64_t>& timestamps = {});
    void setColValue(int row, int column, omnistream::VectorBatch& batch, const nlohmann::json& value);
    void handleFieldError(int row, int column, omnistream::VectorBatch& batch, const std::exception& error);

    std::vector<std::string> fieldNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
    bool ignoreParseErrors;
    bool failOnMissingField;
};
