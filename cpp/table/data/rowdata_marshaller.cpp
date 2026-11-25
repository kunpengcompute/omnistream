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

#include "rowdata_marshaller.h"

void SerializeLongIntoRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos)
{
    if (baseVector->IsNull(rowIdx)) {
        result->setNullAt(pos);
    } else {
        result->setLong(pos, reinterpret_cast<vec::Vector<int64_t>*>(baseVector)->GetValue(rowIdx));
    }
}
void SerializeIntIntoRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos)
{
    if (baseVector->IsNull(rowIdx)) {
        result->setNullAt(pos);
    } else {
        result->setInt(pos, reinterpret_cast<vec::Vector<int32_t>*>(baseVector)->GetValue(rowIdx));
    }
}

void SerializeBooleanIntoRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos)
{
    if (baseVector->IsNull(rowIdx)) {
        result->setNullAt(pos);
    } else {
        result->setBool(pos, reinterpret_cast<vec::Vector<bool>*>(baseVector)->GetValue(rowIdx));
    }
}

void SerializeDecimal128IntoRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos)
{
    if (baseVector->IsNull(rowIdx)) {
        result->setNullAt(pos);
    } else {
        auto value = reinterpret_cast<vec::Vector<Decimal128>*>(baseVector)->GetValue(rowIdx);
        result->setDecimal128(pos, value.LowBits(), value.HighBits());
    }
}

void SerializeVarcharIntoRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos)
{
    if (baseVector->IsNull(rowIdx)) {
        result->setNullAt(pos);
    } else if (baseVector->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        auto casted =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                baseVector);
        auto val = casted->GetValue(rowIdx);
        result->setStringView(pos, val);
    } else if (baseVector->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
        auto casted = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
            baseVector);
        auto val = casted->GetValue(rowIdx);
        result->setStringView(pos, val);
    } else {
        throw std::runtime_error("encoding not supported");
    }
}

void DeserializeIntFromRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* input, int32_t pos)
{
    if (input->isNullAt(pos)) {
        baseVector->SetNull(rowIdx);
    } else {
        reinterpret_cast<vec::Vector<int32_t>*>(baseVector)->SetValue(rowIdx, *input->getInt(pos));
    }
}

void DeserializeLongFromRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* input, int32_t pos)
{
    if (input->isNullAt(pos)) {
        baseVector->SetNull(rowIdx);
    } else {
        reinterpret_cast<vec::Vector<int64_t>*>(baseVector)->SetValue(rowIdx, *input->getLong(pos));
    }
}

void DeserializeVarcharFromRowData(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* input, int32_t pos)
{
    if (input->isNullAt(pos)) {
        baseVector->SetNull(rowIdx);
    } else if (baseVector->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        auto casted =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                baseVector);
        std::string_view sv = input->getStringView(pos);
        casted->SetValue(rowIdx, sv);
    } else if (baseVector->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
        auto casted = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
            baseVector);
        std::string_view sv = input->getStringView(pos);
        casted->SetValue(rowIdx, sv);
    } else {
        throw std::runtime_error("encoding not supported");
    }
}
std::vector<VBToRowSerializer> rowSerializerCenter = {
    nullptr,                        // OMNI_NONE,
    SerializeIntIntoRowData,        // OMNI_INT
    SerializeLongIntoRowData,       // OMNI_LONG
    nullptr,                        // OMNI_DOUBLE
    SerializeBooleanIntoRowData,    // OMNI_BOOLEAN
    nullptr,                        // OMNI_SHORT
    SerializeLongIntoRowData,       // OMNI_DECIMAL64,
    SerializeDecimal128IntoRowData, // OMNI_DECIMAL128
    SerializeIntIntoRowData,        // OMNI_DATE32
    SerializeLongIntoRowData,       // OMNI_DATE64bool
    SerializeIntIntoRowData,        // OMNI_TIME32
    SerializeLongIntoRowData,       // OMNI_TIME64
    SerializeLongIntoRowData,       // OMNI_TIMESTAMP
    nullptr,                        // OMNI_INTERVAL_MONTHS
    nullptr,                        // OMNI_INTERVAL_DAY_TIME
    SerializeVarcharIntoRowData,    // OMNI_VARCHAR
    SerializeVarcharIntoRowData,    // OMNI_CHAR,
    nullptr                         // OMNI_CONTAINER,
};

std::vector<RowToVBDeSerializer> rowDeserializerCenter = {
    nullptr,                       // OMNI_NONE,
    DeserializeIntFromRowData,     // OMNI_INT
    DeserializeLongFromRowData,    // OMNI_LONG
    nullptr,                       // OMNI_DOUBLE
    nullptr,                       // OMNI_BOOLEAN
    nullptr,                       // OMNI_SHORT
    DeserializeLongFromRowData,    // OMNI_DECIMAL64,
    nullptr,                       // OMNI_DECIMAL128
    DeserializeIntFromRowData,     // OMNI_DATE32
    DeserializeLongFromRowData,    // OMNI_DATE64
    DeserializeIntFromRowData,     // /OMNI_TIME32
    DeserializeLongFromRowData,    // OMNI_TIME64
    DeserializeLongFromRowData,    // OMNI_TIMESTAMP
    nullptr,                       // OMNI_INTERVAL_MONTHS
    nullptr,                       // OMNI_INTERVAL_DAY_TIME
    DeserializeVarcharFromRowData, // OMNI_VARCHAR
    DeserializeVarcharFromRowData, // OMNI_CHAR,
    nullptr                        // OMNI_CONTAINER,
};
