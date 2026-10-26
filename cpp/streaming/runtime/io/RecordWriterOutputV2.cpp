/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "RecordWriterOutputV2.h"

#include <runtime/plugable/SerializationDelegate.h>

#include "streaming/runtime/streamrecord/StreamElementSerializer.h"
#include "runtime/io/network/api/CheckpointBarrier.h"

namespace omnistream {
RecordWriterOutputV2::RecordWriterOutputV2(
    RecordWriterV2* recordWriter, TypeSerializer* outSerializer, bool supportsUnalignedCheckpoints)
    : recordWriter_(recordWriter),
      supportsUnalignedCheckpoints_(supportsUnalignedCheckpoints)
{
    LOG(">>>>>");
    if (outSerializer != nullptr) {
        serializationDelegate_ = new SerializationDelegate(new datastream::StreamElementSerializer(outSerializer));
        this->recordWriter_->setSerializationDelegate(serializationDelegate_);
    }
}

RecordWriterOutputV2::~RecordWriterOutputV2()
{
}

bool RecordWriterOutputV2::collectAndCheckIfChained(StreamRecord* record)
{
    LOG(">>>>>>>>");
    pushToRecordWriter(record);
    return true;
}

void RecordWriterOutputV2::pushToRecordWriter(StreamRecord* record)
{
    LOG(">>>>>>> recordWriter_ is " << std::to_string(reinterpret_cast<long>(recordWriter_)));

    if (recordWriter_) {
        recordWriter_->emit(record);
    } else {
        // workaround, do nothing
    }
}

void RecordWriterOutputV2::collect(void* record)
{
    LOG(">>>>Collect " << std::to_string(reinterpret_cast<long>(record)));
    collectAndCheckIfChained(reinterpret_cast<StreamRecord*>(record));
}

void RecordWriterOutputV2::close()
{
    LOG("RecordWriterOutputV2:close >>>>>>>");
}

void RecordWriterOutputV2::emitWatermark(Watermark* watermark)
{
    LOG("RecordWriterOutputV2:emitWatermark >>>>>>>");

    watermarkGauge_.setCurrentwatermark(watermark->getTimestamp());
    // write to all channel of downstream, broadcasting
    if (recordWriter_) {
        recordWriter_->broadcastEmit(watermark);
    }
}

void RecordWriterOutputV2::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    LOG("RecordWriterOutputV2:emitWatermarkStatus >>>>>>>");
}

void RecordWriterOutputV2::broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent)
{
    auto barrier = std::dynamic_pointer_cast<CheckpointBarrier>(event);
    if (barrier != nullptr) {
        LOG_DEBUG(
            "Barrier Iddd: " << barrier->GetId() << ", supportsUnalignedCheckpoints_" << supportsUnalignedCheckpoints_
                             << ", isPriorityEvent: " << isPriorityEvent);
    }

    if (isPriorityEvent && !supportsUnalignedCheckpoints_) {
        if (barrier != nullptr) {
            auto newOptions = barrier->GetCheckpointOptions()->ToAlignedWithTimeout();
            CheckpointBarrier* newBarrier = barrier->WithOptions(newOptions);
            event = std::shared_ptr<AbstractEvent>(newBarrier);
            isPriorityEvent = false;
        }
    }
    recordWriter_->broadcastEvent(event, isPriorityEvent);
}
} // namespace omnistream
