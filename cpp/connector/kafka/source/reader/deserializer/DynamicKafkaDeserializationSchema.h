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

#include <nlohmann/json.hpp>
#include "KafkaRecordDeserializationSchema.h"
#include "core/api/common/serialization/JsonRowDataDeserializationSchema.h"
#include "table/data/vectorbatch/VectorBatch.h"

class DynamicKafkaDeserializationSchema : public KafkaRecordDeserializationSchema {
public:
    explicit DynamicKafkaDeserializationSchema(const nlohmann::json& description);

    void open() override
    {
    }
    void deserialize(RdKafka::Message* record, Collector* out) override;
    void deserialize(std::vector<RdKafka::Message*> records, Collector* out) override;

private:
    std::vector<omniruntime::type::DataTypeId> types;
    JsonRowDataDeserializationSchema rowDeserializer;
    std::vector<int> keyProjection;
    std::vector<int> valueProjection;
    std::vector<std::string> keyNames;
    std::vector<std::string> valueNames;
};
