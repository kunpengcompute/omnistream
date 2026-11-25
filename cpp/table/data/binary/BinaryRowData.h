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

#pragma once

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include "../RowData.h"
#include "../TimestampData.h"
#include "BinarySection.h"
#include "TypedSetters.h"
#include "BinarySegmentUtils.h"

class BinaryRawValueData {
public:
    BinaryRawValueData(const uint8_t* bytes, size_t size)
    {
        if (bytes == nullptr && size != 0) {
            throw std::invalid_argument("RAW value bytes are null");
        }
        if (size != 0) {
            bytes_.assign(bytes, bytes + size);
        }
    }

    const uint8_t* data() const noexcept
    {
        return bytes_.data();
    }
    size_t size() const noexcept
    {
        return bytes_.size();
    }
    std::vector<uint8_t> toBytes() const
    {
        return bytes_;
    }

private:
    std::vector<uint8_t> bytes_;
};

class BinaryRowData : public RowData, public BinarySection, public TypedSetters {
public:
    explicit BinaryRowData(int arity);
    explicit BinaryRowData(int arity, int length);

    ~BinaryRowData() override;

    // virtual
    int getArity() override;

    void setRowKind(RowKind kind) override;

    bool isNullAt(int pos) override;

    RowKind getRowKind() override;

    // Getter and setter
    long* getLong(int pos) override;
    void setLong(int pos, long value) override;
    void setLong(int pos, long* value);

    bool* getBool(int pos) override;
    void setBool(int pos, bool value) override;
    void setBool(int pos, bool* value);

    int* getInt(int pos) override;
    void setInt(int pos, int value) override;

    TimestampData getTimestamp(int pos) override;
    TimestampData getTimestampPrecise(int pos) override;
    void* getRawValue(int pos) override;
    void setTimestamp(int pos, const TimestampData& value, int precision) override;

    BinaryStringData* getString(int pos) override;
    void setString(int pos, BinaryStringData* value) override;

    std::string_view getStringView(int pos) override;
    void setStringView(int pos, std::string_view value) override;

    void setNullAt(int pos);
    void setNotNullAt(int i);

    // Utitilities for `getString`
    void writeFixLenVarchar(int fieldOffset, const uint8_t* bytes, int len);
    void writeVarLenVarchar(int fieldOffset, const uint8_t* bytes, int len);
    int getNumberOfBytesToNearestWord(int numBytes);
    void zeroOutPaddingBytes(int fieldOffset, int numBytes);
    void setOffsetAndSize(int headerOffset, int varcharOffset, int len);
    void setDecimal128(int pos, uint64_t low, int64_t high);
    omniruntime::type::Decimal128* getDecimal128(int pos, int precision);

    // non virtual
    int getFixedLengthPartSize() const;
    void setSizeInBytes(int sizeInBytes);

    bool operator==(const RowData& other) const override;

    int hashCode() const override;
    [[nodiscard]] int hashCodeFast() const override;

    // static
    static int calculateBitSetWidthInBytes(int arity);
    static const int HEADER_SIZE_IN_BITS = 8;
    static int calculateFixPartSizeInBytes(int arity);

    // assume all field are fixed length
    static BinaryRowData* createBinaryRowDataWithMem(int arity);
    static BinaryRowData* createRowFromSubJoinedRows(BinaryRowData* row1, BinaryRowData* row2);
    // for debug
    void printSegInBinary() const;

    int getNullBitsSizeInBytes() const
    {
        return nullBitsSizeInBytes_;
    }

    RowData* copy() override;

private:
    int arity_;
    std::vector<uint8_t> types; // type 1 => long or FixLenVarchar, type 2 => VarLenVarchar
    int nullBitsSizeInBytes_;
    int getFieldOffset(int pos);
    std::vector<std::shared_ptr<BinaryRawValueData>> rawValues_;
};

namespace std {
template <>
struct hash<BinaryRowData> {
    std::size_t operator()(const BinaryRowData& ns) const noexcept
    {
        return ns.hashCodeFast();
    }
};
template <>
struct equal_to<BinaryRowData> {
    bool operator()(const BinaryRowData& lhs, const BinaryRowData& rhs) const noexcept
    {
        return lhs == rhs;
    }
};
template <>
struct hash<BinaryRowData*> {
    std::size_t operator()(const BinaryRowData* nsPtr) const noexcept
    {
        auto hashFast = nsPtr ? nsPtr->hashCodeFast() : 0;
        return static_cast<size_t>(hashFast);
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<BinaryRowData*> {
    bool operator()(const BinaryRowData* lhs, const BinaryRowData* rhs) const noexcept
    {
        return *lhs == *rhs;
    }
};
template <>
struct hash<std::shared_ptr<BinaryRowData>> {
    std::size_t operator()(const std::shared_ptr<BinaryRowData>& nsPtr) const noexcept
    {
        return nsPtr ? nsPtr->hashCodeFast() : 0;
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<std::shared_ptr<BinaryRowData>> {
    bool operator()(const std::shared_ptr<BinaryRowData>& lhs, const std::shared_ptr<BinaryRowData>& rhs) const noexcept
    {
        if (lhs == rhs) return true;
        if (!lhs || !rhs) return false;
        return *lhs == *rhs;
    }
};
template <>
struct hash<std::unique_ptr<BinaryRowData>> {
    std::size_t operator()(const std::unique_ptr<BinaryRowData>& nsPtr) const noexcept
    {
        return nsPtr ? nsPtr->hashCodeFast() : 0;
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<std::unique_ptr<BinaryRowData>> {
    bool operator()(const std::unique_ptr<BinaryRowData>& lhs, const std::unique_ptr<BinaryRowData>& rhs) const noexcept
    {
        if (lhs == rhs) return true;
        if (!lhs || !rhs) return false;
        return *lhs == *rhs;
    }
};
} // namespace std
