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

#ifndef FLINK_TNEL_BINARYROWWRITER_H
#define FLINK_TNEL_BINARYROWWRITER_H

#include "AbstractBinaryWriter.h"
#include "table/data/binary/BinaryRowData.h"

class BinaryRowWriter : public AbstractBinaryWriter {
public:
    explicit BinaryRowWriter(BinaryRowData* row);

    BinaryRowWriter(BinaryRowData* row, int initialSize);
    ~BinaryRowWriter() override = default;

    // virtual
    void writeLong(int pos, long value) override;
    void writeInt(int pos, int value) override;
    void writeString(int pos, std::string_view value) override;
    void writeRawValue(int pos, const uint8_t* bytes, size_t size) override;

    void reset() override;

    void setNullAt(int pos) override;

    void complete() override;

    // non-virtual
    void writeRowKind(RowKind kind);

protected:
    int getFieldOffset(int pos) override;

protected:
    void setNullBit(int ordinal) override;

private:
    // 确保可变长度字段区有 requiredSize 字节的可用空间；必要时扩容并重新绑定 row。
    void ensureVariableCapacity(int requiredSize);
    int nullBitsSizeInBytes_{};
    BinaryRowData* row_{};
    int fixedSize_{};
};

#endif
