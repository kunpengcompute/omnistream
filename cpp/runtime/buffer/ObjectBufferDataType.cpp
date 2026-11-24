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

#include "ObjectBufferDataType.h"
#include "event/EndOfChannelStateEvent.h"

namespace omnistream {

const ObjectBufferDataType ObjectBufferDataType::NONE(false, false, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::DATA_BUFFER(true, false, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::EVENT_BUFFER(false, true, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::PRIORITIZED_EVENT_BUFFER(false, true, false, true, false);
const ObjectBufferDataType ObjectBufferDataType::ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, false);
const ObjectBufferDataType ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, true);
const ObjectBufferDataType ObjectBufferDataType::RECOVERY_COMPLETION(false, true, true, false, false);

ObjectBufferDataType::ObjectBufferDataType()
    : isBuffer_(false),
      isEvent_(false),
      isBlockingUpstream_(false),
      hasPriority_(false),
      requiresAnnouncement_(false)
{
}

ObjectBufferDataType::ObjectBufferDataType(
    bool isBuffer, bool isEvent, bool isBlockingUpstream, bool hasPriority, bool requiresAnnouncement)
    : isBuffer_(isBuffer),
      isEvent_(isEvent),
      isBlockingUpstream_(isBlockingUpstream),
      hasPriority_(hasPriority),
      requiresAnnouncement_(requiresAnnouncement)
{
    if (requiresAnnouncement_ && hasPriority_) {
        throw std::invalid_argument(
            "ObjectBufferDataType has both priority and requires announcement, "
            "which is not supported and doesn't make sense. There should be no need for "
            "announcing priority events, which are always overtaking in-flight data.");
    }
}

ObjectBufferDataType::ObjectBufferDataType(const ObjectBufferDataType& other)
    : isBuffer_(other.isBuffer_),
      isEvent_(other.isEvent_),
      isBlockingUpstream_(other.isBlockingUpstream_),
      hasPriority_(other.hasPriority_),
      requiresAnnouncement_(other.requiresAnnouncement_)
{
}

ObjectBufferDataType& ObjectBufferDataType::operator=(const ObjectBufferDataType& other)
{
    if (this != &other) {
        isBuffer_ = other.isBuffer_;
        isEvent_ = other.isEvent_;
        isBlockingUpstream_ = other.isBlockingUpstream_;
        hasPriority_ = other.hasPriority_;
        requiresAnnouncement_ = other.requiresAnnouncement_;
    }
    return *this;
}

ObjectBufferDataType::~ObjectBufferDataType()
{
}

bool ObjectBufferDataType::isBuffer() const
{
    return isBuffer_;
}

bool ObjectBufferDataType::isEvent() const
{
    return isEvent_;
}

bool ObjectBufferDataType::hasPriority() const
{
    return hasPriority_;
}

bool ObjectBufferDataType::isBlockingUpstream() const
{
    return isBlockingUpstream_;
}

bool ObjectBufferDataType::requiresAnnouncement() const
{
    return requiresAnnouncement_;
}

std::string ObjectBufferDataType::toString() const
{
    std::stringstream ss;
    ss << "ObjectBufferDataType{";
    ss << "isBuffer=" << (isBuffer_ ? "true" : "false") << ", ";
    ss << "isEvent=" << (isEvent_ ? "true" : "false") << ", ";
    ss << "isBlockingUpstream=" << (isBlockingUpstream_ ? "true" : "false") << ", ";
    ss << "hasPriority=" << (hasPriority_ ? "true" : "false") << ", ";
    ss << "requiresAnnouncement=" << (requiresAnnouncement_ ? "true" : "false");
    ss << "}";
    return ss.str();
}

ObjectBufferDataType ObjectBufferDataType::GetDataBufferType(bool hasPriority, std::shared_ptr<AbstractEvent>& event)
{
    if (hasPriority) {
        return ObjectBufferDataType::PRIORITIZED_EVENT_BUFFER;
    }

    if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
        return ObjectBufferDataType::RECOVERY_COMPLETION;
    }

    auto* barrier = dynamic_cast<CheckpointBarrier*>(event.get());
    if (!barrier) {
        return ObjectBufferDataType::EVENT_BUFFER;
    }

    auto opts = barrier->GetCheckpointOptions();
    if (opts->NeedsAlignment()) {
        if (opts->IsTimeoutable()) {
            return ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER;
        }
        return ObjectBufferDataType::ALIGNED_CHECKPOINT_BARRIER;
    }
    return ObjectBufferDataType::EVENT_BUFFER;
}
} // namespace omnistream
