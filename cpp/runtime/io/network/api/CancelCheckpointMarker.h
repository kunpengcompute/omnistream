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

#ifndef OMNISTREAM_CANCELCHECKPOINTMARKER_H
#define OMNISTREAM_CANCELCHECKPOINTMARKER_H

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <memory>
#include "runtime/event/RuntimeEvent.h" // Assume this base class exists

namespace omnistream {

class CancelCheckpointMarker : public RuntimeEvent {
public:
    explicit CancelCheckpointMarker(int64_t checkpointId) : checkpointId_(checkpointId)
    {
    }

    int64_t getCheckpointId() const
    {
        return checkpointId_;
    }

    std::string GetEventClassName() override
    {
        return "CancelCheckpointMarker";
    }

    // These methods mimic the unsupported serialization in Java
    void write(std::ostream& out)
    {
        throw std::runtime_error("write() should never be called on CancelCheckpointMarker");
    }

    void read(std::istream& in)
    {
        throw std::runtime_error("read() should never be called on CancelCheckpointMarker");
    }

    bool operator==(const CancelCheckpointMarker& other) const
    {
        return this->checkpointId_ == other.checkpointId_;
    }

    bool operator!=(const CancelCheckpointMarker& other) const
    {
        return !(*this == other);
    }

    std::string toString()
    {
        return "CancelCheckpointMarker " + std::to_string(checkpointId_);
    }

    std::size_t hashCode()
    {
        auto i = 32;
        return static_cast<std::size_t>(
            static_cast<uint64_t>(checkpointId_) ^ (static_cast<uint64_t>(checkpointId_) >> i));
    }

private:
    int64_t checkpointId_;
};
} // namespace omnistream

#endif // OMNISTREAM_CANCELCHECKPOINTMARKER_H
