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

#ifndef MEMORYSEGMENTUTILS_H
#define MEMORYSEGMENTUTILS_H
#include <libboundscheck/include/securec.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <iostream>

class MemorySegmentUtils {
public:
    static const int ADDRESS_BITS_PER_WORD = 3;
    static const int BIT_BYTE_INDEX_MASK = 7;

    static inline uint8_t* getAll(uint8_t* offHeapBuffer)
    {
        return offHeapBuffer;
    }

    static inline uint8_t get(uint8_t* offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index < size) {
            return *(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void put(uint8_t* offHeapBuffer, int size, int index, uint8_t byte)
    {
        if (index >= 0 && index < size) {
            *(offHeapBuffer + index) = byte;
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void put(uint8_t* offHeapBuffer, int size, int index, const uint8_t* src, int offset, int length)
    {
        if (index >= 0 && index <= size - length) {
            void* pos = static_cast<void*>(offHeapBuffer + index);
            memcpy_s(pos, length, src + offset, length);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::out_of_range("Index out of bound");
        }
    }

    static inline int getSize(int size)
    {
        return size;
    }

    static inline void get(uint8_t* offHeapBuffer, int size, int index, uint8_t* dst, int offset, int length)
    {
        if (index >= 0 && index <= size - length) {
            void* srcPos = static_cast<void*>(offHeapBuffer + index);
            void* dstPos = static_cast<void*>(dst + offset);
            memcpy_s(dstPos, length, srcPos, length);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::out_of_range("Index out of bound");
        }
    }

    static inline int size(int size)
    {
        return size;
    }

    static inline long* getLong(uint8_t* offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(long))) {
            return reinterpret_cast<long*>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putLong(uint8_t* offHeapBuffer, int size, int index, long value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(long))) {
            memcpy_s(offHeapBuffer + index, sizeof(long), &value, sizeof(long));
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline bool* getBool(uint8_t* offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(bool))) {
            return reinterpret_cast<bool*>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putBool(uint8_t* offHeapBuffer, int size, int index, bool value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(bool))) {
            memcpy_s(offHeapBuffer + index, sizeof(bool), &value, sizeof(bool));
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline int* getInt(uint8_t* offHeapBuffer, int size, int index)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(int))) {
            return reinterpret_cast<int*>(offHeapBuffer + index);
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline void putInt(uint8_t* offHeapBuffer, int size, int index, int value)
    {
        if (index >= 0 && index <= size - static_cast<int>(sizeof(int))) {
            memcpy_s(offHeapBuffer + index, sizeof(int), &value, sizeof(int));
        } else {
            std::cerr << "Index: " << index << " size: " << size << std::endl;
            throw std::logic_error("IndexOutOfBoundsException");
        }
    }

    static inline bool equalTo(uint8_t* offHeapBuffer1, uint8_t* offHeapBuffer2, int offset1, int offset2, int length)
    {
        return (std::memcmp(offHeapBuffer1 + offset1, offHeapBuffer2 + offset2, length) == 0);
    }

    static inline int byteIndex(int bitIndex)
    {
        unsigned int ubitIndex = static_cast<unsigned int>(bitIndex);
        return static_cast<int>(ubitIndex >> ADDRESS_BITS_PER_WORD);
    }

    static inline void bitUnSet(uint8_t* offHeapBuffer, int size, int baseOffset, int index)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = get(offHeapBuffer, size, offset);
        unsigned int uindex = static_cast<unsigned int>(index);
        current &= ~(1 << (uindex & BIT_BYTE_INDEX_MASK));
        put(offHeapBuffer, size, offset, current);
    }

    static inline void bitSet(uint8_t* offHeapBuffer, int size, int baseOffset, int index)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = get(offHeapBuffer, size, offset);
        unsigned int uindex = static_cast<unsigned int>(index);
        current |= (1 << (uindex & BIT_BYTE_INDEX_MASK));
        put(offHeapBuffer, size, offset, current);
    }

    static inline void copy(uint8_t* source, int source_offset, uint8_t* dest, int dest_offset, int copy_len)
    {
        memcpy_s(dest + dest_offset, copy_len, source + source_offset, copy_len);
    }
};

#endif // MEMORYSEGMENTUTILS_H
