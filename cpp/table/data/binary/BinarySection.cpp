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

#include "BinarySection.h"
#include "core/include/common.h"
#include "streaming/runtime/tasks/StreamTask.h"

/**
 * public final void pointTo(MemorySegment segment, int offset_, int sizeInBytes) {
       pointTo(new MemorySegment[] {segment}, offset_, sizeInBytes);
   }

 * @param segment
 * @param offset
 * @param sizeInBytes
 */

BinarySection::~BinarySection()
{
    LOG("destructor  BinarySection");
    if (owner_ == 1 && memoryBuffer && bufferCapacity > 0) {
        delete[] memoryBuffer;
    }
}

void BinarySection::pointTo(uint8_t* segment, int offset, int sizeInBytes, int bufferCapacity_)
{
    memoryBuffer = segment;
    offset_ = offset;
    sizeInBytes_ = sizeInBytes;
    this->bufferCapacity = bufferCapacity_;
}

void BinarySection::own(uint8_t* segment, int offset, int sizeInBytes, int bufferCapacity_)
{
    memoryBuffer = segment;
    offset_ = offset;
    sizeInBytes_ = sizeInBytes;
    this->bufferCapacity = bufferCapacity_;
    owner_ = 1;
}

int BinarySection::getSizeInBytes() const
{
    return sizeInBytes_;
}

int BinarySection::getOffset() const
{
    return offset_;
}

uint8_t* BinarySection::getSegment() const
{
    return memoryBuffer;
}

int BinarySection::getBufferCapacity() const
{
    return bufferCapacity;
}

void BinarySection::changeOwner(int owner)
{
    owner_ = owner;
}
