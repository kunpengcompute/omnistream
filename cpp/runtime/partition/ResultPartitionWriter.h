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

#ifndef OMNISTREAM_RESULT_PARTITION_WRITER_H
#define OMNISTREAM_RESULT_PARTITION_WRITER_H

#include <memory>
#include <vector>
#include <future>
#include <string>
#include <sstream>
#include <iostream>
#include <stdexcept>
#include <optional>
#include <event/AbstractEvent.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <io/AvailabilityProvider.h>
#include <utils/lang/AutoCloseable.h>

#include "BufferAvailabilityListener.h"
#include "ResultSubpartitionView.h"
#include "io/network/api/StopMode.h"
#include "metrics/groups/TaskIOMetricGroup.h"

namespace omnistream {

class ResultPartitionWriter : public AvailabilityProvider, public AutoCloseable {
public:
    virtual ~ResultPartitionWriter() = default;

    virtual void setup() = 0;

    virtual ResultPartitionIDPOD getPartitionId() = 0;

    virtual int getNumberOfSubpartitions() = 0;

    virtual int getNumTargetKeyGroups() = 0;

    virtual void emitRecord(void* record, int targetSubpartition) = 0;

    virtual void broadcastRecord(void* record) = 0;

    virtual void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent) = 0;

    virtual void NotifyEndOfData(StopMode mode) = 0;

    virtual std::shared_ptr<CompletableFuture> getAllDataProcessedFuture() = 0;

    virtual std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
        int index, BufferAvailabilityListener* availabilityListener) = 0;

    virtual void flushAll() = 0;

    virtual void flush(int subpartitionIndex) = 0;

    virtual void fail(std::optional<std::exception_ptr> throwable) = 0;

    virtual void finish() = 0;

    virtual bool isFinished() = 0;

    virtual void release(std::optional<std::exception_ptr> cause) = 0;

    virtual bool isReleased() = 0;

    virtual void cancel() = 0;
    virtual void close() = 0;

    virtual std::string toString() const
    {
        std::stringstream ss;
        ss << "ResultPartitionWriter";
        return ss.str();
    }
    virtual void SetMetricGroup(std::shared_ptr<AbstractMetricGroup> metricGroup)
    {
    }
};

} // namespace omnistream

#endif // OMNISTREAM_RESULT_PARTITION_WRITER_H
