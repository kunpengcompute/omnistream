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

#include "RowKind.h"
#include "../types/logical/LogicalType.h"
#include "FieldGetter.h"
#include "TimestampData.h"
#include "binary/BinaryStringData.h"
#include "table/data/binary/MurmurHashUtils.h"
#include <functional>
#include <cstddef>

inline std::size_t hash_combine(std::size_t lhs, std::size_t rhs)
{
    lhs ^= rhs + 0x9e3779b9 + (lhs << 6) + (lhs >> 2);
    return lhs;
}
class RowData {
public:
    explicit RowData(int rowDataTypeId);

    virtual ~RowData() = default;

    // virutal
    /**
     * Returns the number of fields in this row.
     *
     * <p>The number does not include {@link RowKind}. It is kept separately.
     */
    virtual int getArity() = 0;

    /**
     * Sets the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    virtual void setRowKind(RowKind kind) = 0;

    /** Returns true if the field is null at the given position. */
    virtual bool isNullAt(int pos) = 0;

    virtual long* getLong(int pos) = 0;

    virtual bool* getBool(int pos) = 0;

    virtual int* getInt(int pos) = 0;

    virtual BinaryStringData* getString(int pos);

    virtual RowKind getRowKind() = 0;

    virtual TimestampData getTimestamp(int pos) = 0;

    virtual TimestampData getTimestampPrecise(int pos) = 0;

    virtual void setLong(int pos, long value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void setBool(int pos, bool value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void setInt(int pos, int value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void setTimestamp(int pos, const TimestampData& value, int precision)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void setString(int pos, BinaryStringData* value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual std::string_view getStringView(int pos)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void* getRawValue(int pos)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual void setStringView(int pos, std::string_view value)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual bool operator==(const RowData& other) const = 0;
    virtual int hashCode() const = 0;
    virtual int hashCodeFast() const = 0;
    // constant
    static const int BinaryRowDataID = 0;
    static const int GenericRowDataID = 1;
    static const int JoinedRowDataID = 2;

    // non virtual
    [[nodiscard]] int getRowDataTypeId() const;

    static FieldGetter* createFieldGetter(LogicalType* fieldType, int fieldPos);

    void printRow();

    virtual RowData* copy()
    {
        return nullptr;
    };

private:
    int rowDataTypeID_{-1};
};

namespace std {
template <>
struct hash<RowData> {
    std::size_t operator()(const RowData& ns) const noexcept
    {
        return ns.hashCodeFast();
    }
};
template <>
struct equal_to<RowData> {
    bool operator()(const RowData& lhs, const RowData& rhs) const noexcept
    {
        return lhs == rhs;
    }
};
template <>
struct hash<RowData*> {
    std::size_t operator()(const RowData* nsPtr) const noexcept
    {
        return nsPtr->hashCodeFast();
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<RowData*> {
    bool operator()(const RowData* lhs, const RowData* rhs) const noexcept
    {
        return *lhs == *rhs;
    }
};
template <>
struct hash<std::shared_ptr<RowData>> {
    std::size_t operator()(const std::shared_ptr<RowData>& nsPtr) const noexcept
    {
        return nsPtr ? nsPtr->hashCodeFast() : 0;
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<std::shared_ptr<RowData>> {
    bool operator()(const std::shared_ptr<RowData>& lhs, const std::shared_ptr<RowData>& rhs) const noexcept
    {
        if (lhs == rhs) return true;
        if (!lhs || !rhs) return false;
        return *lhs == *rhs;
    }
};
template <>
struct hash<std::unique_ptr<RowData>> {
    std::size_t operator()(const std::unique_ptr<RowData>& nsPtr) const noexcept
    {
        return nsPtr ? nsPtr->hashCodeFast() : 0;
    }
};
// Attention: Be very careful when using this! It does not compare the address. but the content
template <>
struct equal_to<std::unique_ptr<RowData>> {
    bool operator()(const std::unique_ptr<RowData>& lhs, const std::unique_ptr<RowData>& rhs) const noexcept
    {
        if (lhs == rhs) return true;
        if (!lhs || !rhs) return false;
        return *lhs == *rhs;
    }
};
} // namespace std
