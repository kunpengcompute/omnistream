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

#ifndef FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H
#define FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H

#include <vector>
#include <memory>
#include <optional>
#include <stdexcept>
#include <cstdint>
#include <nlohmann/json.hpp>
#include "data/vectorbatch/VectorBatch.h"
#include "table/data/RowData.h"
#include "KafkaRecordSerializationSchema.h"
#include "table/data/GenericRowData.h"
#include "table/data/Row.h"
#include "KeyValueByteContainer.h"
#include "basictypes/String.h"

class DynamicKafkaRecordSerializationSchema : public KafkaRecordSerializationSchema<RowData> {
public:
    using MetadataPositions = std::vector<int>;

    DynamicKafkaRecordSerializationSchema() {};

    DynamicKafkaRecordSerializationSchema(std::vector<std::string>& inputFields, std::vector<std::string>& inputTypes);
    explicit DynamicKafkaRecordSerializationSchema(const nlohmann::json& description);

    struct UpsertRecord {
        std::string key;
        std::optional<std::string> value;
    };

    UpsertRecord serializeUpsert(omnistream::VectorBatch* input, int rowIndex);
    void RowToJson(RowData* row);
    void RowToJson(omnistream::VectorBatch* input, int rowIndex);
    KeyValueByteContainer Serialize(RowData* consumedRow);
    KeyValueByteContainer Serialize(omnistream::VectorBatch* input, int rowIndex);
    KeyValueByteContainer Serialize(String* element);
    KeyValueByteContainer Serialize(Row* row);

private:
    std::vector<FieldGetter> keyFieldGetters_;
    std::vector<FieldGetter> valueFieldGetters_;
    bool hasMetadata_;
    MetadataPositions metadataPositions_;
    bool upsertMode_;
    std::vector<std::string> inputFields_;
    std::vector<std::string> inputTypes_;
    std::vector<std::pair<int32_t, int32_t>> decimalInfo;
    std::vector<omniruntime::type::DataTypeId> physicalTypes;
    std::vector<int> keyProjection;
    std::vector<int> valueProjection;
    std::vector<std::string> keyFieldNames;
    std::vector<std::string> valueFieldNames;
    std::string serializeProjection(
        omnistream::VectorBatch* input,
        int rowIndex,
        const std::vector<int>& projection,
        const std::vector<std::string>& names);
    nlohmann::ordered_json j;
    std::ostringstream oss;
    char timeBuffer[80];
    static GenericRowData createProjectedRow(RowData& sourceRow, RowKind kind, std::vector<FieldGetter>& getters);
};

#endif // FLINK_BENCHMARK_DYNAMICKAFKARECORDSERIALIZATIONSCHEMA_H
