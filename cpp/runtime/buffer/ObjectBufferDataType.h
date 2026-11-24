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

#ifndef OMNISTREAM_VECTORBATCHBUFFERDATATYPE_H
#define OMNISTREAM_VECTORBATCHBUFFERDATATYPE_H

#include <string>
#include <memory>
#include <sstream>
#include <stdexcept>
#include "event/AbstractEvent.h"
#include "io/network/api/CheckpointBarrier.h"

namespace omnistream {

class ObjectBufferDataType {
public:
    static const ObjectBufferDataType NONE;
    static const ObjectBufferDataType DATA_BUFFER;
    static const ObjectBufferDataType EVENT_BUFFER;
    static const ObjectBufferDataType PRIORITIZED_EVENT_BUFFER;
    static const ObjectBufferDataType ALIGNED_CHECKPOINT_BARRIER;
    static const ObjectBufferDataType TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER;
    static const ObjectBufferDataType RECOVERY_COMPLETION;

    ObjectBufferDataType();
    ObjectBufferDataType(
        bool isBuffer, bool isEvent, bool isBlockingUpstream, bool hasPriority, bool requiresAnnouncement);
    ObjectBufferDataType(const ObjectBufferDataType& other);
    ObjectBufferDataType& operator=(const ObjectBufferDataType& other);
    ~ObjectBufferDataType();

    bool isBuffer() const;
    bool isEvent() const;
    bool hasPriority() const;
    bool isBlockingUpstream() const;
    bool requiresAnnouncement() const;

    friend bool operator==(const ObjectBufferDataType& lhs, const ObjectBufferDataType& rhs)
    {
        return lhs.isBuffer_ == rhs.isBuffer_ && lhs.isEvent_ == rhs.isEvent_ &&
               lhs.isBlockingUpstream_ == rhs.isBlockingUpstream_ && lhs.hasPriority_ == rhs.hasPriority_ &&
               lhs.requiresAnnouncement_ == rhs.requiresAnnouncement_;
    }

    friend bool operator!=(const ObjectBufferDataType& lhs, const ObjectBufferDataType& rhs)
    {
        return !(lhs == rhs);
    }

    std::string toString() const;

    static ObjectBufferDataType GetDataBufferType(bool hasPriority, std::shared_ptr<AbstractEvent>& event);

private:
    bool isBuffer_;
    bool isEvent_;
    bool isBlockingUpstream_;
    bool hasPriority_;
    bool requiresAnnouncement_;
};

} // namespace omnistream

#endif // OMNISTREAM_VECTORBATCHBUFFERDATATYPE_H
