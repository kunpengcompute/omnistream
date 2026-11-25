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

#ifndef OMNIFLINK_BINARYSECTION_H
#define OMNIFLINK_BINARYSECTION_H

#include <cstdint>
// forward declaration
template <typename U>
class LazyBinaryFormat;

// Current memory segment is treated as a single byte array holding uint8_t.
// In java, this would be split into multiple segments_ each holding a chunk of data that will be reassembled
// upon a getBytes function call. The offset_ would indicate the position of the starting position in the first segment,
// then the rest of the segments_ are fully used until we reach the set number of sizeInBytes.

// Important: BinarySection does not own Segments, it only ref the segment. The actor object who use BinarySection and
// its descendants should provide the segments and own the segments.
class BinarySection {
    template <typename U>
    friend class LazyBinaryFormat;

public:
    BinarySection() = default;
    // BinarySection(MemorySegment *segments[], int numSegments, int offset, int sizeInBytes) :
    // segments_(segments), numSegments_(numSegments), offset_(offset), sizeInBytes_(sizeInBytes) {};
    virtual ~BinarySection();

    BinarySection(uint8_t* segment, int offset, int sizeInBytes)
        : memoryBuffer(segment),
          offset_(offset),
          sizeInBytes_(sizeInBytes) {};

    void pointTo(uint8_t* segment, int offset, int sizeInBytes, int bufferCapacity);
    void own(uint8_t* segment, int offset, int sizeInBytes, int bufferCapacity);

    int getSizeInBytes() const;

    // MemorySegment **getSegments() const;

    int getOffset() const;

    // int getNumSegments() const;
    uint8_t* getSegment() const;
    int getBufferCapacity() const;
    void changeOwner(int owner);

protected:
    // Assume we're using segment[0] as just a simple byte array, and offset_ is usually 0
    // MemorySegment ** segments_;
    uint8_t* memoryBuffer;
    int offset_ = 0;
    int sizeInBytes_ = 0;

    int bufferCapacity = 0;
    int owner_ = 0; // 0 default not own mem,  1 own the mem
};

#endif // OMNIFLINK_BINARYSECTION_H
