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

// PipelinedSubpartitionView.cpp
#include "PipelinedSubpartitionView.h"
#include <sstream>
#include <stdexcept>
#include "PipelinedSubpartition.h"
#include "BufferAvailabilityListener.h"
#include "ResultSubpartition.h"
#include "AvailabilityWithBacklog.h"
#include "core/include/common.h"

namespace omnistream {

PipelinedSubpartitionView::PipelinedSubpartitionView(
    std::shared_ptr<PipelinedSubpartition> parent, BufferAvailabilityListener* listener)
    : parent(parent),
      availabilityListener(listener),
      isReleased_(false)
{
    if (!parent || !listener) {
        throw std::invalid_argument("parent and listener cannot be null");
    }
}

PipelinedSubpartitionView::PipelinedSubpartitionView()
    : parent(nullptr),
      availabilityListener(nullptr),
      isReleased_(false)
{
}

PipelinedSubpartitionView::~PipelinedSubpartitionView()
{
}

BufferAndBacklog* PipelinedSubpartitionView::getNextBuffer()
{
    // LOG_TRACE(">>> beginnning of get NextBuffer")
    if (isReleased_.load() || parent == nullptr) {
        return nullptr;
    }
    return parent->pollBuffer();
}

void PipelinedSubpartitionView::notifyDataAvailable()
{
    if (isReleased_.load() || availabilityListener == nullptr) {
        return;
    }
    LOG("PipelinedSubpartitionView notifyDataAvailable invoke!");
    availabilityListener->notifyDataAvailable();
}

void PipelinedSubpartitionView::notifyPriorityEvent(int priorityBufferNumber)
{
    if (isReleased_.load() || availabilityListener == nullptr) {
        return;
    }
    availabilityListener->notifyPriorityEvent(priorityBufferNumber);
}

void PipelinedSubpartitionView::ConvertToPriorityEvent(int sequenceNumber)
{
    if (!parent) {
        return;
    }
    parent->ConvertToPriorityEvent(sequenceNumber);
}

void PipelinedSubpartitionView::releaseAllResources()
{
    bool expected = false;
    bool desired = true;
    if (isReleased_.compare_exchange_strong(expected, desired)) {
        availabilityListener = nullptr;
        if (parent != nullptr) {
            parent->onConsumedSubpartition();
        }
    }
}

bool PipelinedSubpartitionView::isReleased()
{
    return isReleased_.load() || parent == nullptr || parent->isReleased();
}

void PipelinedSubpartitionView::resumeConsumption()
{
    parent->resumeConsumption();
}

void PipelinedSubpartitionView::acknowledgeAllDataProcessed()
{
    parent->acknowledgeAllDataProcessed();
}

AvailabilityWithBacklog PipelinedSubpartitionView::getAvailabilityAndBacklog(int numCreditsAvailable)
{
    return parent->getAvailabilityAndBacklog(numCreditsAvailable);
}

std::shared_ptr<std::exception> PipelinedSubpartitionView::getFailureCause()
{
    return {};
}

int PipelinedSubpartitionView::unsynchronizedGetNumberOfQueuedBuffers()
{
    return parent->unsynchronizedGetNumberOfQueuedBuffers();
}

int PipelinedSubpartitionView::getNumberOfQueuedBuffers()
{
    return parent->getNumberOfQueuedBuffers();
}

void PipelinedSubpartitionView::notifyNewBufferSize(int newBufferSize)
{
    parent->bufferSize(newBufferSize);
}

std::string PipelinedSubpartitionView::toString()
{
    std::stringstream ss;
    ss << this->getClassSimpleName() << "(index: " << parent->getSubPartitionIndex() << ") of ResultPartition ";
    return ss.str();
}

std::string PipelinedSubpartitionView::getClassSimpleName()
{
    return "PipelinedSubpartitionView";
}

} // namespace omnistream
