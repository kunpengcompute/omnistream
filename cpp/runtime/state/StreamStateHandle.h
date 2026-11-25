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

#ifndef FLINK_TNEL_STREAMSTATEHANDLE_H
#define FLINK_TNEL_STREAMSTATEHANDLE_H

#include <memory>
#include <vector>
#include <optional>
#include "core/fs/FSDataInputStream.h"
#include "runtime/state/StateObject.h"
#include "runtime/state/PhysicalStateHandleID.h"

/**
 * A StateObject that represents state that was written to a stream.
 * The data can be read back via openInputStream().
 */
class StreamStateHandle : public StateObject {
public:
    virtual ~StreamStateHandle() = default;

    /**
     * Returns an FSDataInputStream that can be used to read back the data
     * that was previously written to the stream.
     *
     * @return unique_ptr to FSDataInputStream
     * @throws std::ios_base::failure if stream opening fails
     */
    virtual std::shared_ptr<FSDataInputStream> OpenInputStream() const = 0;

    /**
     * @return Content of this handle as a byte array if it is already in memory.
     */
    virtual std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const = 0;

    /**
     * @return a unique identifier of this handle.
     */
    virtual PhysicalStateHandleID GetStreamStateHandleID() const = 0;

    virtual bool operator==(const StreamStateHandle& other) const = 0;

    virtual void DiscardState() = 0;

private:
    int bufferSize_;
    std::string filePath;
};
#endif // FLINK_TNEL_STREAMSTATEHANDLE_H
