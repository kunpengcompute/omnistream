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

#include "BinaryRowWriter.h"
#include <limits>
#include "../../../core/memory/MemorySegmentUtils.h"

BinaryRowWriter::BinaryRowWriter(BinaryRowData* row, int initialSize) : row_(row)
{
    nullBitsSizeInBytes_ = BinaryRowData::calculateBitSetWidthInBytes(row->getArity());
    fixedSize_ = row->getFixedLengthPartSize();
    cursor_ = fixedSize_;

    int buffer_size = fixedSize_ + initialSize;

    memoryBuffer = new uint8_t[buffer_size];

    // Transfer ownership to row_. This keeps the backing buffer valid even if the writer is destroyed first.
    row_->own(memoryBuffer, 0, buffer_size, buffer_size);
}

BinaryRowWriter::BinaryRowWriter(BinaryRowData* row) : BinaryRowWriter(row, 0)
{
}

void BinaryRowWriter::writeLong(int pos, long value)
{
    MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);
}

void BinaryRowWriter::writeRowKind(RowKind kind)
{
    row_->setRowKind(kind);
}

void BinaryRowWriter::writeInt(int pos, int value)
{
    MemorySegmentUtils::putInt(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), value);
}

void BinaryRowWriter::reset()
{
    cursor_ = fixedSize_;
    for (int i = 0; i < nullBitsSizeInBytes_; i += 8) {
        row_->setLong(i, 0L);
        MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), i, 0L);
    }
}

void BinaryRowWriter::setNullAt(int pos)
{
    setNullBit(pos);
    MemorySegmentUtils::putLong(memoryBuffer, row_->getBufferCapacity(), getFieldOffset(pos), 0L);
}

void BinaryRowWriter::setNullBit(int pos)
{
    row_->setNullAt(pos);
}

int BinaryRowWriter::getFieldOffset(int pos)
{
    return nullBitsSizeInBytes_ + 8 * pos;
}

void BinaryRowWriter::complete()
{
    row_->setSizeInBytes(cursor_);
}

void BinaryRowWriter::writeString(int pos, std::string_view value)
{
    // 对齐 Java AbstractBinaryWriter.writeString 的语义：
    //   ≤7 字节：直接写入 8 字节固定槽位（header + 内联数据）
    //   >7 字节：固定槽写入 offset+len，内容写入以 cursor_ 为起点的可变区
    // 不使用 BinaryRowData::setStringView：其 writeVarLenVarchar 会 delete[] 并重新分配
    // writer 与 row 共享的 buffer，导致 writer->memoryBuffer 悬垂。
    const int fieldOffset = getFieldOffset(pos);
    const int len = static_cast<int>(value.size());
    const auto* bytes = reinterpret_cast<const uint8_t*>(value.data());
    if (len <= 7) {
        row_->setNotNullAt(pos);
        row_->writeFixLenVarchar(fieldOffset, bytes, len);
    } else {
        const int roundedSize = row_->getNumberOfBytesToNearestWord(len);
        ensureVariableCapacity(roundedSize);
        row_->setNotNullAt(pos);
        row_->setOffsetAndSize(fieldOffset, cursor_, len);
        MemorySegmentUtils::put(memoryBuffer, row_->getBufferCapacity(), cursor_, bytes, 0, len);
        row_->zeroOutPaddingBytes(cursor_, len);
        cursor_ += roundedSize;
    }
}

void BinaryRowWriter::writeRawValue(int pos, const uint8_t* bytes, size_t size)
{
    if (bytes == nullptr && size != 0) {
        throw std::invalid_argument("RAW value bytes are null");
    }
    if (size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw std::length_error("RAW value is too large");
    }
    const int len = static_cast<int>(size);
    const int roundedSize = row_->getNumberOfBytesToNearestWord(len);
    ensureVariableCapacity(roundedSize);
    row_->setNotNullAt(pos);
    row_->setOffsetAndSize(getFieldOffset(pos), cursor_, len);
    if (len != 0) {
        MemorySegmentUtils::put(memoryBuffer, row_->getBufferCapacity(), cursor_, bytes, 0, len);
    }
    row_->zeroOutPaddingBytes(cursor_, len);
    cursor_ += roundedSize;
}

void BinaryRowWriter::ensureVariableCapacity(int requiredSize)
{
    const int required = cursor_ + requiredSize;
    if (required <= row_->getBufferCapacity()) {
        return;
    }
    const int doubled = row_->getBufferCapacity() * 2;
    const int newCapacity = required > doubled ? required : doubled;
    auto* newBuffer = new uint8_t[newCapacity]();
    auto ret = memcpy_s(newBuffer, newCapacity, memoryBuffer, row_->getBufferCapacity());
    if (ret != EOK) {
        delete[] newBuffer;
        throw std::runtime_error("memcpy_s failed in BinaryRowWriter::ensureVariableCapacity");
    }
    delete[] memoryBuffer;
    memoryBuffer = newBuffer;
    // Keep row_ as the sole owner of the replacement buffer.
    row_->own(memoryBuffer, 0, cursor_, newCapacity);
}
