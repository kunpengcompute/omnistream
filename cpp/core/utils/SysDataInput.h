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

#ifndef FLINK_TNEL_SYSDATAINPUT_H
#define FLINK_TNEL_SYSDATAINPUT_H

#include <cstdint>
#include <string>

class SysDataInput {
public:
    virtual int readUnsignedByte() = 0;
    virtual uint8_t readByte() = 0;
    virtual ~SysDataInput() = default;

    virtual int64_t readLong() = 0;
    virtual int readInt() = 0;
    virtual double readDouble() = 0;
    virtual std::string readUTF() = 0;
    virtual int readUnsignedShort() = 0;
    virtual bool readBoolean() = 0;
    // from the input stream read into the buffer of  capacity, beginning at offset of buffer,
    // and read length bytes
    virtual void readFully(uint8_t* buffer, int capacity, int offset, int length) = 0;

    virtual const uint8_t* getData() = 0;
    virtual size_t getPosition() = 0;
    virtual void setPosition(size_t) = 0;
};

#endif // FLINK_TNEL_SYSDATAINPUT_H
