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

#ifndef FLINK_TNEL_FSDATAINPUTSTREAM_H
#define FLINK_TNEL_FSDATAINPUTSTREAM_H

#include <iostream>

/**
 * Interface for a data input stream to a file on a custom FileSystem.
 *
 * This extends std::istream with methods for accessing the stream's
 * current position and seeking to a desired position.
 */
class FSDataInputStream {
public:
    virtual ~FSDataInputStream() = default;

    /**
     * Seek to the given offset from the start of the file. The next read()
     * will be from that location. Can't seek past the end of the stream.
     *
     * @param desired the desired offset
     * @throws std::ios_base::failure if seeking fails
     */
    virtual void Seek(std::streampos desired) = 0;

    /**
     * Gets the current position in the input stream.
     *
     * @return current position in the input stream
     * @throws std::ios_base::failure if getting position fails
     */
    virtual std::streampos GetPos() const = 0;

    virtual int Read() = 0;
    virtual int Read(std::vector<uint8_t>& buffer, int off, int len) = 0;
};

#endif // FLINK_TNEL_FSDATAINPUTSTREAM_H
