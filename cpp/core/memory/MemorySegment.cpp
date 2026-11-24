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

#include "MemorySegment.h"
#include <libboundscheck/include/securec.h>
#include <cstring>
#include <stdexcept>

#include "../include/common.h"

MemorySegment::MemorySegment(uint8_t* offHeapBuffer, int size)
    : Segment(SegmentType::MEMORY_SEGMENT),
      offHeapBuffer_(offHeapBuffer),
      size_(size),
      owner_(nullptr)
{
}

MemorySegment::MemorySegment(uint8_t* offHeapBuffer, int size, void* owner)
    : Segment(SegmentType::MEMORY_SEGMENT),
      offHeapBuffer_(offHeapBuffer),
      size_(size),
      owner_(owner)
{
}

MemorySegment::MemorySegment(int size) : Segment(SegmentType::MEMORY_SEGMENT), size_(size)
{
    offHeapBuffer_ = new uint8_t[size];
    owner_ = nullptr;
}

uint8_t* MemorySegment::getAll()
{
    return offHeapBuffer_;
}

void* MemorySegment::getOwner()
{
    return owner_;
}

uint8_t MemorySegment::get(int index)
{
    if (index >= 0 && index < size_) {
        return *(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

uint8_t* MemorySegment::getData()
{
    return offHeapBuffer_;
}

/**
 * Write a single `byte` to the memory segment at the specified `index`
 */
void MemorySegment::put(int index, uint8_t byte)
{
    if (index >= 0 && index < size_) {
        *(offHeapBuffer_ + index) = byte;
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

/**
 * Write a sequence of bytes `src` of `length` to the memory segment at the specified `index`
 */
void MemorySegment::put(int index, const uint8_t* src, int offset, int length)
{
    if (index >= 0 && index <= size_ - length) {
        uint8_t* pos = static_cast<uint8_t*>(offHeapBuffer_ + index);
        std::copy(src + offset, src + offset + length, pos);
        /*auto ret = memcpy_s(pos, length, src + offset, length);
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed, ret: " + std::to_string(ret));
        }*/
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        throw std::out_of_range("Index out of bound");
    }
}

int MemorySegment::getSize() const
{
    return size_;
}

/**
 * Bulk get method. Copies a sequence of bytes of length `length` from the memory segment starting at the specified
 * index to the destination memory `dst`, beginning at the given `offset`
 */
void MemorySegment::get(int index, uint8_t* dst, int offset, int length)
{
    if (index >= 0 && index <= size_ - length) {
        auto ret = memcpy_s(dst + offset, length, offHeapBuffer_ + index, length);
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

int MemorySegment::size()
{
    return size_;
}

long* MemorySegment::getLong(int index)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(long)) {
        return reinterpret_cast<long*>(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

void MemorySegment::putLong(int index, long value)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(long)) {
        auto ret = memcpy_s(offHeapBuffer_ + index, sizeof(long), &value, sizeof(long));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

int* MemorySegment::getInt(int index)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(int)) {
        return reinterpret_cast<int*>(offHeapBuffer_ + index);
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

void MemorySegment::putInt(int index, int value)
{
    if (index >= 0 && static_cast<size_t>(index) <= size_ - sizeof(int)) {
        auto ret = memcpy_s(offHeapBuffer_ + index, sizeof(int), &value, sizeof(int));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
    } else {
        std::cerr << "Index: " << index << " size: " << size_ << std::endl;
        THROW_LOGIC_EXCEPTION("IndexOutOfBoundsException");
    }
}

/**
 * Equals two memory segment regions.
 *
 * @param seg2 Segment to equal this segment with
 * @param offset1 Offset of this segment to start equaling
 * @param offset2 Offset of seg2 to start equaling
 * @param length Length of the equaled memory region
 * @return true if equal, false otherwise
 */
bool MemorySegment::equalTo(MemorySegment seg2, int offset1, int offset2, int length)
{
    return (memcmp((offHeapBuffer_ + offset1), (seg2.offHeapBuffer_ + offset2), length) == 0);
}

MemorySegment::~MemorySegment()
{
    if (!owner_) {
        delete[] offHeapBuffer_;
    }
}
