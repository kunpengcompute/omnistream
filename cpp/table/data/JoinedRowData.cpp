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

#include "JoinedRowData.h"
#include <functional>
#include <stdexcept>

JoinedRowData::JoinedRowData()
    : RowData(RowData::JoinedRowDataID),
      rowKind(RowKind::INSERT),
      row1(nullptr),
      row2(nullptr)
{
}

JoinedRowData::JoinedRowData(RowData* row1_, RowData* row2_)
    : RowData(RowData::JoinedRowDataID),
      rowKind(RowKind::INSERT),
      row1(row1_),
      row2(row2_)
{
}

JoinedRowData::JoinedRowData(RowKind rowKind_, RowData* row1_, RowData* row2_)
    : RowData(RowData::JoinedRowDataID),
      rowKind(rowKind_),
      row1(row1_),
      row2(row2_)
{
}

JoinedRowData* JoinedRowData::replace(RowData* row1_, RowData* row2_)
{
    this->row1 = row1_;
    this->row2 = row2_;
    return this;
}

int JoinedRowData::getArity()
{
    if (!row1 && !row2) {
        INFO_RELEASE("both rows are null");
        throw std::runtime_error("rows are null");
    } else if (!row1 && row2) {
        INFO_RELEASE("row1 is null");
        return row2->getArity();
    } else if (!row2 && row1) {
        INFO_RELEASE("row2 is null");
        return row1->getArity();
    }
    return row1->getArity() + row2->getArity();
}

RowKind JoinedRowData::getRowKind()
{
    return rowKind;
}

void JoinedRowData::setRowKind(RowKind kind)
{
    rowKind = kind;
}

bool JoinedRowData::isNullAt(int pos)
{
    if (!row1) {
        INFO_RELEASE("row1 is null");
        return true;
    }
    if (pos < row1->getArity()) {
        return row1->isNullAt(pos);
    } else {
        if (!row2) {
            INFO_RELEASE("row2 is null");
            return true;
        }
        return row2->isNullAt(pos - row1->getArity());
    }
}

long* JoinedRowData::getLong(int pos)
{
    if (!row1) {
        INFO_RELEASE("row1 is null");
        return nullptr;
    }
    if (pos < row1->getArity()) {
        return row1->getLong(pos);
    } else {
        if (!row2) {
            INFO_RELEASE("row2 is null");
            return nullptr;
        }
        return row2->getLong(pos - row1->getArity());
    }
}

bool* JoinedRowData::getBool(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getBool(pos);
    } else {
        return row2->getBool(pos - row1->getArity());
    }
}

RowData* JoinedRowData::getRow1()
{
    return row1;
}

RowData* JoinedRowData::getRow2()
{
    return row2;
}

TimestampData JoinedRowData::getTimestamp(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getTimestamp(pos);
    } else {
        return row2->getTimestamp(pos - row1->getArity());
    }
}

TimestampData JoinedRowData::getTimestampPrecise(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getTimestampPrecise(pos);
    } else {
        return row2->getTimestampPrecise(pos - row1->getArity());
    }
}

int* JoinedRowData::getInt(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getInt(pos);
    } else {
        return row2->getInt(pos - row1->getArity());
    }
}

int JoinedRowData::hashCode() const
{
    return hash_combine(row1->hashCode(), row2->hashCode());
}

bool JoinedRowData::operator==(const RowData& other) const
{
    // Check for self-reference
    if (this == &other) {
        return true;
    }

    auto castedOther = dynamic_cast<const JoinedRowData*>(&other);
    if (castedOther == nullptr) {
        // If it is not compared to a BinaryRowData return false directly
        return false;
    }

    return this->row1 == castedOther->row1 && this->row2 == castedOther->row2;
}

void JoinedRowData::setRow1(RowData* row1_)
{
    this->row1 = row1_;
}

void JoinedRowData::setRow2(RowData* row2_)
{
    this->row2 = row2_;
}

void JoinedRowData::setLong(int pos, long value)
{
    if (pos < row1->getArity()) {
        row1->setLong(pos, value);
    } else {
        row2->setLong(pos - row1->getArity(), value);
    }
}

void JoinedRowData::setInt(int pos, int value)
{
    if (pos < row1->getArity()) {
        row1->setInt(pos, value);
    } else {
        row2->setInt(pos - row1->getArity(), value);
    }
}

void JoinedRowData::setTimestamp(int pos, const TimestampData& value, int precision)
{
    if (pos < row1->getArity()) {
        row1->setTimestamp(pos, value, precision);
    } else {
        row2->setTimestamp(pos - row1->getArity(), value, precision);
    }
}

void JoinedRowData::setString(int pos, BinaryStringData* value)
{
    if (pos < row1->getArity()) {
        row1->setString(pos, value);
    } else {
        row2->setString(pos - row1->getArity(), value);
    }
}

int JoinedRowData::hashCodeFast() const
{
    int row1Hash = (row1 != nullptr) ? row1->hashCodeFast() : 0;
    int row2Hash = (row2 != nullptr) ? row2->hashCodeFast() : 0;
    return hash_combine(row1Hash, row2Hash);
}

std::string_view JoinedRowData::getStringView(int pos)
{
    if (!row1) {
        return std::string_view();
    }
    if (pos < row1->getArity()) {
        return row1->getStringView(pos);
    } else {
        if (!row2) {
            return std::string_view();
        }
        return row2->getStringView(pos - row1->getArity());
    }
}

void JoinedRowData::setStringView(int pos, std::string_view value)
{
    if (pos < row1->getArity()) {
        row1->setStringView(pos, value);
    } else {
        row2->setStringView(pos - row1->getArity(), value);
    }
}

void* JoinedRowData::getRawValue(int pos)
{
    if (pos < row1->getArity()) {
        return row1->getRawValue(pos);
    }
    return row2->getRawValue(pos - row1->getArity());
}
