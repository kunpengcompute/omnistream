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

#include <stdarg.h>
#include <memory>
#include "RowData.h"

class GenericRowData : public RowData {
public:
    GenericRowData(int arity) : RowData(RowData::GenericRowDataID), arity_(arity)
    {
        fields_.resize(arity);
        typeIDs_.resize(arity);
        null_.resize(arity, false);
    }
    GenericRowData(std::vector<int>& typeIDs, RowKind kind)
        : RowData(RowData::GenericRowDataID),
          kind_(kind),
          arity_(typeIDs.size()),
          typeIDs_(typeIDs)
    {
        fields_.resize(arity_);
        null_.resize(arity_, false);
    }

    GenericRowData(std::vector<int>& typeIDs)
        : RowData(RowData::GenericRowDataID),
          kind_(RowKind::INSERT),
          arity_(typeIDs.size()),
          typeIDs_(typeIDs)
    {
        fields_.resize(arity_);
        null_.resize(arity_, false);
    }

    ~GenericRowData() override = default;

    /** Returns the long value at the given position. */
    long* getLong(int pos) override
    {
        return &fields_[pos];
    };

    bool* getBool(int pos) override
    {
        return reinterpret_cast<bool*>(fields_[pos]);
    };

    int* getInt(int pos) override
    {
        return reinterpret_cast<int*>(fields_.data() + pos);
    }

    TimestampData getTimestamp(int pos) override
    {
        return *reinterpret_cast<TimestampData*>(fields_[pos]);
    };

    TimestampData getTimestampPrecise(int pos) override
    {
        return *reinterpret_cast<TimestampData*>(fields_[pos]);
    };

    void* getRawValue(int pos) override
    {
        return reinterpret_cast<void*>(fields_[pos]);
    }

    // non virtual
    [[nodiscard]] int getRowDataTypeId() const
    {
        return RowData::GenericRowDataID;
    };

    template <typename... Args>
    GenericRowData* of(std::vector<int>& typeIDs, Args... args)
    {
        GenericRowData* rowData = new GenericRowData(typeIDs);
        rowData->setFieldandProceedPos(args...);
        curPos = 0;
        return rowData;
    }

    template <typename... Args>
    GenericRowData* ofKind(RowKind kind, std::vector<int>& typeIDs, Args... args)
    {
        GenericRowData* rowData = new GenericRowData(typeIDs, kind);
        rowData->setFieldandProceedPos(args...);
        curPos = 0;
        return rowData;
    }

    template <typename T>
    void setField(int pos, T value)
    {
        if (pos < 0 || pos >= arity_) {
            throw std::out_of_range("GenericRowData::setField position out of range");
        }
        if constexpr (std::is_same<T, std::nullptr_t>::value) {
            null_[pos] = true;
        } else if constexpr (!std::is_pointer<T>::value) {
            fields_[pos] = value;
            null_[pos] = false;
        } else {
            fields_[pos] = reinterpret_cast<long>(value);
            null_[pos] = false;
        }
    }

    long getField(int pos) const
    {
        return fields_[pos];
    }

    int getArity() override
    {
        return arity_;
    }

    RowKind getRowKind() override
    {
        return kind_;
    }

    void setRowKind(RowKind kind) override
    {
        kind_ = kind;
    }

    bool isNullAt(int pos) override
    {
        if (pos < 0 || pos >= arity_) {
            return true;
        }
        return null_[pos];
    }

    std::vector<int> getTypeIDs() const
    {
        return typeIDs_;
    }

    int hashCode() const override
    {
        NOT_IMPL_EXCEPTION;
    }
    int hashCodeFast() const override
    {
        NOT_IMPL_EXCEPTION;
    };
    bool operator==(const RowData& other) const
    {
        NOT_IMPL_EXCEPTION;
    }

private:
    /** The kind of change that a row describes in a changelog. */
    RowKind kind_ = RowKind::INSERT;
    int arity_;
    // either a pointer or a primitive value
    std::vector<long> fields_;
    // typeID
    std::vector<int> typeIDs_;

    std::vector<bool> null_;

    int curPos = 0;
    template <typename T>
    void setFieldandProceedPos(T value)
    {
        setField(curPos, value);
        curPos++;
    }
};
