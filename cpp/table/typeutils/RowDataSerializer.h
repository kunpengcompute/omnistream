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

#ifndef FLINK_TNEL_ROWDATASERIALIZER_H
#define FLINK_TNEL_ROWDATASERIALIZER_H

#include <vector>

#include "../../core/typeutils/TypeSerializerSingleton.h"
#include "BinaryRowDataSerializer.h"
#include "../types/logical/LogicalType.h"
#include "../types/logical/RowType.h"
#include "../data/binary/BinaryRowData.h"
#include "../data/writer/BinaryRowWriter.h"

class RowDataSerializer : public TypeSerializerSingleton {
public:
    explicit RowDataSerializer(omnistream::RowType* rowType);
    ~RowDataSerializer() override;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    [[nodiscard]] const char* getName() const override;

    BinaryRowData* toBinaryRow(RowData* row);

    BackendDataType getBackendId() const override
    {
        return BackendDataType::ROW_BK;
    };

    int getArity() const
    {
        return static_cast<int>(types_.size());
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::ROW};
        typeJson.logicalType = rowType_;

        return typeJson.toJson();
    }

private:
    BinaryRowDataSerializer binarySerializer_;
    std::vector<LogicalType*> types_;
    std::vector<TypeSerializer*> fieldSerializers_;
    std::vector<FieldGetter*> fieldGetters_;
    omnistream::RowType* rowType_;

    BinaryRowData* reuseRow_;
    BinaryRowWriter* reuseWriter_;
};

#endif
