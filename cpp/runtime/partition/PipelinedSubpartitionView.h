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

// PipelinedSubpartitionView.h
#ifndef PIPELINEDSUBPARTITIONVIEW_H
#define PIPELINEDSUBPARTITIONVIEW_H

#include <memory>
#include <atomic>
#include <string>

#include "AvailabilityWithBacklog.h"
#include "BufferAndBacklog.h"
#include "BufferAvailabilityListener.h"
#include "ResultSubpartitionView.h"

namespace omnistream {
class PipelinedSubpartition;

class PipelinedSubpartitionView : public ResultSubpartitionView {
public:
    PipelinedSubpartitionView(std::shared_ptr<PipelinedSubpartition> parent, BufferAvailabilityListener* listener);
    PipelinedSubpartitionView();
    ~PipelinedSubpartitionView() override;

    BufferAndBacklog* getNextBuffer() override;
    void notifyDataAvailable() override;
    void notifyPriorityEvent(int priorityBufferNumber) override;
    void ConvertToPriorityEvent(int sequenceNumber) override;
    void releaseAllResources() override;
    bool isReleased() override;
    void resumeConsumption() override;
    void acknowledgeAllDataProcessed() override;
    AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) override;
    std::shared_ptr<std::exception> getFailureCause() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    int getNumberOfQueuedBuffers() override;
    void notifyNewBufferSize(int newBufferSize) override;
    std::string toString();
    std::string getClassSimpleName();

private:
    std::shared_ptr<PipelinedSubpartition> parent;
    BufferAvailabilityListener* availabilityListener;
    std::atomic<bool> isReleased_;
};

} // namespace omnistream

#endif // PIPELINEDSUBPARTITIONVIEW_H
