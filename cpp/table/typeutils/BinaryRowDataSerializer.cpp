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

#include "BinaryRowDataSerializer.h"
#include "../data/binary/BinarySegmentUtils.h"

namespace {
constexpr int MAX_ROW_SIZE_IN_BYTES = 64 * 1024 * 1024;
}

BinaryRowDataSerializer::BinaryRowDataSerializer(int numFields) : numFields_(numFields)
{
    fixedLengthPartSize_ = BinaryRowData::calculateFixPartSizeInBytes(numFields_);
    reUse_ = new BinaryRowData(numFields_);
    auto* bytes = new uint8_t[SEG_SIZE];
    reUse_->own(bytes, 0, SEG_SIZE, SEG_SIZE);
}

BinaryRowDataSerializer::BinaryRowDataSerializer(int numFields, const std::vector<std::string>& inputTypes)
    : numFields_(numFields)
{
    inputTypes_ = inputTypes;
    fixedLengthPartSize_ = BinaryRowData::calculateFixPartSizeInBytes(numFields_);
    reUse_ = new BinaryRowData(numFields_);
    auto* bytes = new uint8_t[SEG_SIZE];
    reUse_->own(bytes, 0, SEG_SIZE, SEG_SIZE);
}

BinaryRowDataSerializer::~BinaryRowDataSerializer()
{
    delete reUse_;
}

void* BinaryRowDataSerializer::deserialize(DataInputView& source)
{
    int length = source.readInt();

    source.validateLength(length, MAX_ROW_SIZE_IN_BYTES);

    if (length > reUse_->getBufferCapacity()) {
        auto* expandedRow = new BinaryRowData(numFields_);
        auto* expandedBytes = new uint8_t[length];
        expandedRow->own(expandedBytes, 0, length, length);
        delete reUse_;
        reUse_ = expandedRow;
    }

    auto bytes = reUse_->getSegment();
    LOG(" bytes: " << reinterpret_cast<long>(bytes) << " capacity: " << length << " offset : " << 0
                   << " length: " << length);
    source.readFully(bytes, reUse_->getBufferCapacity(), 0, length);
    // PRINT_HEX(bytes, 0, length)
    reUse_->setSizeInBytes(length);

    return reUse_;
}

void BinaryRowDataSerializer::serialize(void* row, DataOutputSerializer& target)
{
    LOG(">>>>");
    // We eventually needs to implement a JoinedRowDataSerializer
    // in Q4, Agg/Join are never the last Op in chain, so JoinedRowDataSerialzier is not needed of Q4.
    auto* castedRow = reinterpret_cast<RowData*>(row);
    if (castedRow->getRowDataTypeId() == RowData::JoinedRowDataID) {
        INFO_RELEASE("WARNING: joinedRowToBinaryRow are only meant for testing. It only treat long type!!");
        reUse_ = joinedRowToBinaryRow(static_cast<JoinedRowData*>(row));
    } else {
        auto* binRow = reinterpret_cast<BinaryRowData*>(row);
        LOG(">>>>" << binRow->getSizeInBytes());
        target.writeInt(binRow->getSizeInBytes());

        LOG("getBufferCapacity>>>>" << binRow->getBufferCapacity() << " , getOffset :" << binRow->getOffset());

        BinarySegmentUtils::copyToView(
            binRow->getSegment(), binRow->getBufferCapacity(), binRow->getOffset(), binRow->getSizeInBytes(), target);
    }
}

void BinaryRowDataSerializer::serialize(Object* row, DataOutputSerializer& target)
{
    LOG(">>>>");
    // We eventually needs to implement a JoinedRowDataSerializer
    // in Q4, Agg/Join are never the last Op in chain, so JoinedRowDataSerialzier is not needed of Q4.
    auto* castedRow = reinterpret_cast<RowData*>(static_cast<void*>(row));
    if (castedRow->getRowDataTypeId() == RowData::JoinedRowDataID) {
        INFO_RELEASE("WARNING: joinedRowToBinaryRow are only meant for testing. It only treat long type!!");
    } else {
        auto* binRow = reinterpret_cast<BinaryRowData*>(row);
        LOG(">>>>" << binRow->getSizeInBytes());
        target.writeInt(binRow->getSizeInBytes());

        LOG("getBufferCapacity>>>>" << binRow->getBufferCapacity() << " , getOffset :" << binRow->getOffset());

        BinarySegmentUtils::copyToView(
            binRow->getSegment(), binRow->getBufferCapacity(), binRow->getOffset(), binRow->getSizeInBytes(), target);
    }
}

const char* BinaryRowDataSerializer::getName() const
{
    return "BinaryRowDataSerializer";
}

// The caller takes ownership of the returned pointer
BinaryRowData* BinaryRowDataSerializer::joinedRowToBinaryRow(
    JoinedRowData* row, const std::vector<omniruntime::type::DataTypeId>& typeId)
{
    if (typeId.empty()) {
        INFO_RELEASE("WARNING: joinedRowToBinaryRow are only meant for testing. It only treat long type!!");
    }
    if (!row) {
        GErrorLog("JoinedRowData is null");
        throw std::runtime_error("JoinedRowData is null");
    }
    // by default, all fields are long or timestamp. If there are other type, typeId need to be provided.
    // Eventually, we will have a JoinedRowDataSerializer. This is just for testing purposes
    BinaryRowData* newRow = BinaryRowData::createBinaryRowDataWithMem(row->getArity());
    newRow->setRowKind(row->getRowKind());
    if (!typeId.empty()) {
        if (typeId.size() != static_cast<size_t>(row->getArity())) {
            throw std::runtime_error("joinedRowToBinaryRow: typeId size does not match row arity");
        }
    }
    for (int i = 0; i < newRow->getArity(); i++) {
        if (row->isNullAt(i)) {
            newRow->setNullAt(i);
            continue;
        }
        if (typeId.empty() || typeId[i] == omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
            typeId[i] == omniruntime::type::OMNI_LONG || typeId[i] == omniruntime::type::OMNI_TIMESTAMP ||
            typeId[i] == omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            long* longValue = row->getLong(i);
            if (longValue != nullptr) {
                newRow->setLong(i, *longValue);
            } else {
                throw std::runtime_error("Long value is null at index " + std::to_string(i));
            }
        } else if (
            !typeId.empty() &&
            (typeId[i] == omniruntime::type::OMNI_VARCHAR || typeId[i] == omniruntime::type::OMNI_CHAR)) {
            newRow->setStringView(i, row->getStringView(i));
        } else {
            throw std::runtime_error("type not supported!");
        }
    }
    return newRow;
}

BinaryRowData* BinaryRowDataSerializer::joinedRowFromBothBinaryRow(JoinedRowData* row)
{
    // Both  row1 and row2  are of type BinaryRowData
    if (!dynamic_cast<BinaryRowData*>(row->getRow1())) {
        throw std::runtime_error("row1 is not BinaryRowData type");
    }
    if (!dynamic_cast<BinaryRowData*>(row->getRow2())) {
        throw std::runtime_error("row2 is not BinaryRowData type");
    }
    auto* row1 = dynamic_cast<BinaryRowData*>(row->getRow1());
    auto* row2 = dynamic_cast<BinaryRowData*>(row->getRow2());
    BinaryRowData* newRow = BinaryRowData::createRowFromSubJoinedRows(row1, row2);
    newRow->setRowKind(row->getRowKind());
    return newRow;
}

const std::vector<std::string>& BinaryRowDataSerializer::getInputTypes() const
{
    return inputTypes_;
}
