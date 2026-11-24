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

#include "table/data/RowData.h"
#include "table/data/TimestampData.h"
#include "table/data/RowKind.h"

class JoinedRowData : public RowData {
public:
    JoinedRowData();
    JoinedRowData(RowData* row1_, RowData* row2_);
    JoinedRowData(RowKind rowKind_, RowData* row1_, RowData* row2_);

    ~JoinedRowData() override = default;

    JoinedRowData* replace(RowData* row1_, RowData* row2_);

    int getArity() override;
    RowKind getRowKind() override;
    void setRowKind(RowKind kind) override;
    bool isNullAt(int pos) override;
    long* getLong(int pos) override;
    bool* getBool(int pos) override;
    int* getInt(int pos) override;
    TimestampData getTimestamp(int pos) override;
    TimestampData getTimestampPrecise(int pos) override;
    void* getRawValue(int pos) override;
    std::string_view getStringView(int pos);
    void setStringView(int pos, std::string_view value);

    RowData* getRow1();
    RowData* getRow2();

    void setRow1(RowData* row1_);
    void setRow2(RowData* row2_);
    void setLong(int pos, long value) override;

    void setInt(int pos, int value) override;

    void setTimestamp(int pos, const TimestampData& value, int precision) override;

    void setString(int pos, BinaryStringData* value) override;

    int hashCode() const override;
    [[nodiscard]] int hashCodeFast() const override;
    bool operator==(const RowData& other) const override;

private:
    RowKind rowKind;
    RowData* row1 = nullptr;
    RowData* row2 = nullptr;
};
