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

#include "RowDataSerializer.h"
#include "InternalSerializers.h"
#include "BinaryRowDataSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

RowDataSerializer::RowDataSerializer(omnistream::RowType* rowType)
    : binarySerializer_(static_cast<int>(rowType->getChildren().size())),
      reuseRow_(nullptr),
      reuseWriter_(nullptr)
{
    rowType_ = new omnistream::RowType(*rowType);
    auto types = rowType_->getChildren();
    types_ = types;

    for (size_t i = 0; i < types.size(); i++) {
        LogicalType* fieldType = types_[i];
        TypeSerializer* serializer = InternalSerializers::create(fieldType);
        fieldSerializers_.push_back(serializer);
        fieldGetters_.push_back(RowData::createFieldGetter(fieldType, i));
    }
}

RowDataSerializer::~RowDataSerializer()
{
    delete reuseRow_;
    delete reuseWriter_;
    delete rowType_;
}

void* RowDataSerializer::deserialize(DataInputView& source)
{
    LOG(">>>>");
    return binarySerializer_.deserialize(source);
}

void RowDataSerializer::serialize(void* rowData, DataOutputSerializer& target)
{
    LOG(">>>>");
    binarySerializer_.serialize(toBinaryRow(static_cast<RowData*>(rowData)), target);
}

BinaryRowData* RowDataSerializer::toBinaryRow(RowData* row)
{
    LOG(">>>>");

    if (row->getRowDataTypeId() == RowData::BinaryRowDataID) {
        LOG(">>>>");
        return static_cast<BinaryRowData*>(row);
    }

    if (reuseRow_ == nullptr) {
        reuseRow_ = new BinaryRowData(types_.size());
        reuseWriter_ = new BinaryRowWriter(reuseRow_);
    }

    reuseWriter_->reset();
    reuseWriter_->writeRowKind(row->getRowKind());
    for (size_t i = 0; i < types_.size(); i++) {
        if (row->isNullAt(i)) {
            reuseWriter_->setNullAt(i);
        } else {
            auto typeId = types_[i]->getTypeId();
            if (typeId == omniruntime::type::DataTypeId::OMNI_VARCHAR ||
                typeId == omniruntime::type::DataTypeId::OMNI_CHAR) {
                // VARCHAR/CHAR 字段按 std::string_view 读取并写入可变长度字段，
                // 避免走 BinaryWriter::write 的 void* object 路径（无字符串长度信息）。
                reuseWriter_->writeString(static_cast<int>(i), row->getStringView(static_cast<int>(i)));
            } else {
                BinaryWriter::write(
                    reuseWriter_,
                    static_cast<int>(i),
                    (fieldGetters_)[i]->getFieldOrNull(row),
                    types_[i],
                    (fieldSerializers_)[i]);
            }
        }
    }
    reuseWriter_->complete();
    return reuseRow_;
}

const char* RowDataSerializer::getName() const
{
    return "RowDataSerializer";
}
