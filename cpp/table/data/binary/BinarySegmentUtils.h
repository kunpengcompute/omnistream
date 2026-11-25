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

#ifndef FLINK_TNEL_BINARYSEGMENTUTILS_H
#define FLINK_TNEL_BINARYSEGMENTUTILS_H

#include <iostream>
#include <sstream>
#include "MurmurHashUtils.h"
#include "../../../core/memory/MemorySegmentUtils.h"
#include <map>
#include "table/data/TimestampData.h"
#include "table/data/binary/BinaryStringData.h"

class BinarySegmentUtils {
public:
    // static  std::map<int, uint8_t *> BYTES_LOCAL;
    // static  std::map<int, int> BYTES_LOCAL_LENGTH;

    static inline uint8_t* allocateReuseBytes(int length)
    {
        uint8_t* bytes = BYTES_LOCAL[0];
        unsigned int ulength = static_cast<unsigned int>(length);
        if (bytes == nullptr) {
            if (ulength <= MAX_BYTES_LENGTH) {
                bytes = new uint8_t[MAX_BYTES_LENGTH];
                BYTES_LOCAL[0] = bytes;
                BYTES_LOCAL_LENGTH[0] = MAX_BYTES_LENGTH;
            } else {
                bytes = new uint8_t[ulength];
                BYTES_LOCAL[0] = bytes;
                BYTES_LOCAL_LENGTH[0] = length;
            }
        } else if (MAX_BYTES_LENGTH < ulength) {
            delete BYTES_LOCAL[0];
            bytes = new uint8_t[length];
            BYTES_LOCAL[0] = bytes;
            BYTES_LOCAL_LENGTH[0] = length;
        }
        return bytes;
    }

    static inline void copyToView(
        uint8_t* segment, int segmentSize, int offset, int sizeInBytes, DataOutputSerializer& target)
    {
        target.write(segment, segmentSize, offset, sizeInBytes);
    }

    static inline void bitUnSet(uint8_t* offHeapBuffer, int baseOffset, int index, int size)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = MemorySegmentUtils::get(offHeapBuffer, size, offset);
        unsigned int uindex = static_cast<unsigned int>(index);
        current &= ~(1 << (uindex & BIT_BYTE_INDEX_MASK));
        MemorySegmentUtils::put(offHeapBuffer, size, offset, current);
    }

    static inline void bitSet(uint8_t* offHeapBuffer, int baseOffset, int index, int size)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = MemorySegmentUtils::get(offHeapBuffer, size, offset);
        unsigned int uindex = static_cast<unsigned int>(index);
        current |= (1 << (uindex & BIT_BYTE_INDEX_MASK));
        MemorySegmentUtils::put(offHeapBuffer, size, offset, current);
    }

    static inline long getLong(uint8_t* segment, int segmentSize, int offset)
    {
        return *MemorySegmentUtils::getLong(segment, segmentSize, offset);
    }

    static inline void setLong(uint8_t* segments, int segmentSize, int offset, long value)
    {
        MemorySegmentUtils::putLong(segments, segmentSize, offset, value);
    }

    static inline int byteIndex(int bitIndex)
    {
        unsigned int ubitIndex = static_cast<unsigned int>(bitIndex);
        return ubitIndex >> ADDRESS_BITS_PER_WORD;
    }

    static inline bool bitGet(uint8_t* segment, int segmentSize, int baseOffset, int index)
    {
        int offset = baseOffset + byteIndex(index);
        uint8_t current = MemorySegmentUtils::get(segment, segmentSize, offset);
        unsigned int uindex = static_cast<unsigned int>(index);
        return (current & (1 << (uindex & BIT_BYTE_INDEX_MASK))) != 0;
    }

    /**
     * Read string data from the given segment
     *
     * @param segment the memory segment
     * @param baseOffset the base offset of `BinaryRowData`
     * @param fieldOffset the field offset of the current column
     * @param variablePartOffsetAndLen
     *    - If `mark` is set, the lower 32 bits represent `subOffset` and the higher 32 bits represent `len`. E.g.
     * `variablePartOffsetAndLen` with underlying memory `08 00 00 00 28 00 00 00`, has `subOffset` = 40 and `len` = 8
     *    - If `mark` is not set, the higher 56 bits represent the content and the lowest 4 bits represent `len`. Thus,
     * the variable name "variablePartOffsetAndLen" is not accurate in this case
     *
     * Memory layout of VARCHAR column varies depending on the length of the content.
     *
     * Setter sees `setString` in "OmniFlink/cpp/table/data/binary/BinaryRowData.cpp"
     */

    static inline BinaryStringData* readStringData(
        uint8_t* segment, int baseOffset, int fieldOffset, int64_t variablePartOffsetAndLen)
    {
        uint64_t uvariablePartOffsetAndLen = static_cast<uint64_t>(variablePartOffsetAndLen);
        int64_t mark = uvariablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            // VARCHAR more than 7 characters
            int subOffset = static_cast<int>(uvariablePartOffsetAndLen >> 32);
            int len = static_cast<int>(variablePartOffsetAndLen);
            // return BinaryStringData::fromAddress(segment, baseOffset + subOffset, len);
            return BinaryStringData::fromBytes(segment, baseOffset + subOffset, len);
        } else { // VARCHAR less than or equal to 7 characters
            int len = static_cast<int>((uvariablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56);
            if (LITTLE_ENDIAN) {
                return BinaryStringData::fromBytes(segment, fieldOffset, len);
            } else {
                // fieldOffset + 1 to skip header.
                return BinaryStringData::fromBytes(segment, fieldOffset + 1, len);
            }
        }
    }

    static std::string_view readStringView(
        uint8_t* segment, int baseOffset, int fieldOffset, int64_t variablePartOffsetAndLen)
    {
        uint64_t uvariablePartOffsetAndLen = static_cast<uint64_t>(variablePartOffsetAndLen);
        int64_t mark = uvariablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            // VARCHAR more than 7 characters
            int subOffset = static_cast<int>(uvariablePartOffsetAndLen >> 32);
            int len = static_cast<int>(variablePartOffsetAndLen);
            // return BinaryStringData::fromAddress(segment, baseOffset + subOffset, len);
            return std::string_view(reinterpret_cast<const char*>(segment) + baseOffset + subOffset, len);
        } else {
            // VARCHAR less than or equal to 7 characters
            int len = static_cast<int>((uvariablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56);
            if (LITTLE_ENDIAN) {
                return std::string_view(reinterpret_cast<const char*>(segment) + fieldOffset, len);
            } else {
                // fieldOffset + 1 to skip header.
                return std::string_view(reinterpret_cast<const char*>(segment) + fieldOffset + 1, len);
            }
        }
    }

    /**
     * hash segments to int, numBytes must be aligned to 4 bytes.
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to hash.
     */

    static inline int hashByWords(uint8_t* segment, int offset, int numBytes)
    {
        return MurmurHashUtils::hashBytesByWords(segment, offset, numBytes);
    }

    static inline int hashBytes(uint8_t* segment, int offset, int numBytes)
    {
        return MurmurHashUtils::hashBytes(segment, offset, numBytes);
    }

    /**
     * Equals two memory segments regions.
     *
     * @param segments1 Segments 1
     * @param offset1 Offset of segments1 to start equaling
     * @param segments2 Segments 2
     * @param offset2 Offset of segments2 to start equaling
     * @param len Length of the equaled memory region
     * @return true if equal, false otherwise
     */
    static inline bool equals(uint8_t* segments1, int offset1, uint8_t* segments2, int offset2, int len)
    {
        return std::memcmp(segments1 + offset1, segments2 + offset2, len) == 0;
    }

private:
    static std::map<int, uint8_t*> BYTES_LOCAL;
    static std::map<int, int> BYTES_LOCAL_LENGTH;
    static const unsigned int MAX_BYTES_LENGTH = 1024 * 64;
    static const unsigned int BIT_BYTE_INDEX_MASK = 7;
    static const unsigned int ADDRESS_BITS_PER_WORD = 3;
    static const uint64_t HIGHEST_FIRST_BIT = 0x80L << 56;
    static const uint64_t HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;
    static const int BYTE_ARRAY_BASE_OFFSET = 0;

    static int hashMultiSegByWords(uint8_t* segments, int numSegments, int offset, int numBytes);
    static bool inFirstSegment(uint8_t* segments, int offset, int numBytes);
};

#endif // FLINK_TNEL_BINARYSEGMENTUTILS_H
