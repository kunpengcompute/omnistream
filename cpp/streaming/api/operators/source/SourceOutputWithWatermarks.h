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

#pragma once

#include <memory>
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include <utility>
#include <stdexcept>
#include <algorithm>
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "core/api/common/eventtime/WatermarkOutput.h"
#include "core/api/common/eventtime/TimestampAssigner.h"
#include "core/api/common/eventtime/WatermarkGenerator.h"
#include "core/api/connector/source/ReaderOutput.h"
#include "data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

class SourceOutputWithWatermarks : public ReaderOutput {
public:
    ~SourceOutputWithWatermarks() override
    {
        delete reusingRecord_;
        delete watermarkGenerator_;
    }

    // SourceOutput Methods
    void Collect(void* record) override
    {
        Collect(record, TimestampAssigner::NO_TIMESTAMP);
    }

    void Collect(void* record, long timestamp) override
    {
        if (idleTimeoutMillis_ > 0) {
            lastRecordTime_ = timeService_->getCurrentProcessingTime();
            MarkActive();
        }
        if (timestampAssigner_->getRowtimeFieldIndex() < 0) {
            // TODO: Datastream scenario, not correct now
            try {
                const long assignedTimestamp = timestampAssigner_->ExtractTimestamp(record, timestamp);

                // IMPORTANT: The event must be emitted before the watermark generator is called.
                reusingRecord_->replace(record, assignedTimestamp);
                recordsOutput_->emitRecord(reusingRecord_);
                watermarkGenerator_->OnEvent(record, assignedTimestamp, onEventWatermarkOutput_);
            } catch (const std::exception& e) {
                THROW_RUNTIME_ERROR(e.what());
            }
        } else {
            // SQL scenario
            auto vectorBatch = static_cast<omnistream::VectorBatch*>(record);
            try {
                CollectVectorBatch(vectorBatch);
            } catch (const std::exception& e) {
                THROW_RUNTIME_ERROR("SourceOutputWithWatermarks failed to collect VectorBatch: " << e.what());
            }
        }
    }

    // WatermarkOutput Methods
    void emitWatermark(Watermark* watermark) override
    {
        onEventWatermarkOutput_->emitWatermark(watermark);
    }

    void MarkIdle() override
    {
        isIdle_ = true;
        onEventWatermarkOutput_->MarkIdle();
    }

    void MarkActive() override
    {
        isIdle_ = false;
        onEventWatermarkOutput_->MarkActive();
    }

    SourceOutput& CreateOutputForSplit(const std::string& splitId)
    {
        NOT_IMPL_EXCEPTION;
    }

    void ReleaseOutputForSplit(const std::string& splitId)
    {
        NOT_IMPL_EXCEPTION;
    }

    void ConfigureIdleness(ProcessingTimeService* timeService, int64_t timeoutMillis)
    {
        timeService_ = timeService;
        idleTimeoutMillis_ = timeoutMillis;
        lastRecordTime_ = timeService ? timeService->getCurrentProcessingTime() : 0;
    }

    void EmitPeriodicWatermark()
    {
        if (idleTimeoutMillis_ > 0 &&
            timeService_->getCurrentProcessingTime() - lastRecordTime_ >= idleTimeoutMillis_) {
            if (!isIdle_) {
                watermarkGenerator_->OnPeriodicEmit(periodicWatermarkOutput_);
                MarkIdle();
            }
            return;
        }
        watermarkGenerator_->OnPeriodicEmit(periodicWatermarkOutput_);
    }

    static SourceOutputWithWatermarks* createWithSeparateOutputs(
        OmniDataOutputPtr recordsOutput,
        WatermarkOutput* onEventWatermarkOutput,
        WatermarkOutput* periodicWatermarkOutput,
        TimestampAssigner* timestampAssigner,
        WatermarkGenerator* watermarkGenerator,
        long autoWatermarkInterval);

protected:
    SourceOutputWithWatermarks(
        OmniDataOutputPtr recordsOutput,
        WatermarkOutput* onEventWatermarkOutput,
        WatermarkOutput* periodicWatermarkOutput,
        TimestampAssigner* timestampAssigner,
        WatermarkGenerator* watermarkGenerator,
        long autoWatermarkInterval)
        : recordsOutput_(recordsOutput),
          onEventWatermarkOutput_(onEventWatermarkOutput),
          periodicWatermarkOutput_(periodicWatermarkOutput),
          timestampAssigner_(timestampAssigner),
          watermarkGenerator_(watermarkGenerator),
          autoWatermarkInterval_(autoWatermarkInterval)
    {
        reusingRecord_ = new StreamRecord();
    }

private:
    void CollectVectorBatch(omnistream::VectorBatch* vectorBatch)
    {
        if (vectorBatch == nullptr) {
            THROW_RUNTIME_ERROR("SourceOutputWithWatermarks received a null VectorBatch.");
        }

        const int32_t rowtimeFieldIndex = timestampAssigner_->getRowtimeFieldIndex();
        if (rowtimeFieldIndex < 0 || rowtimeFieldIndex >= vectorBatch->GetVectorCount()) {
            THROW_RUNTIME_ERROR("Invalid rowtime field index for VectorBatch.");
        }

        auto timeColumn = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatch->Get(rowtimeFieldIndex));
        const int32_t rowCount = vectorBatch->GetRowCount();
        bool splitBatch = false;
        int32_t offset = 0;
        long maxTimestampInBatch = 0;

        for (int32_t i = 0; i < rowCount; i++) {
            if (timeColumn->IsNull(i)) {
                THROW_RUNTIME_ERROR("RowTime field should not be null.");
            }

            const long assignedTimestamp = timeColumn->GetValue(i);
            maxTimestampInBatch = std::max(maxTimestampInBatch, assignedTimestamp);
            if (assignedTimestamp - watermarkGenerator_->getLastEmittedMaxTimestamp() > autoWatermarkInterval_) {
                splitBatch = true;
                const int32_t newRowCnt = i + 1 - offset;
                omnistream::VectorBatch* pBatch =
                    omnistream::VectorBatchUtil::sliceVectorBatch(vectorBatch, offset, newRowCnt);

                // IMPORTANT: The event must be emitted before the watermark generator is called.
                reusingRecord_->replace(pBatch, assignedTimestamp);
                recordsOutput_->emitRecord(reusingRecord_);
                watermarkGenerator_->OnEvent(pBatch, assignedTimestamp, onEventWatermarkOutput_);
                watermarkGenerator_->OnPeriodicEmit(onEventWatermarkOutput_);
                offset = i + 1;
            }
        }

        if (splitBatch) {
            const int32_t newRowCnt = rowCount - offset;
            if (newRowCnt > 0) {
                omnistream::VectorBatch* pBatch =
                    omnistream::VectorBatchUtil::sliceVectorBatch(vectorBatch, offset, newRowCnt);
                reusingRecord_->replace(pBatch, maxTimestampInBatch);
                recordsOutput_->emitRecord(reusingRecord_);
                watermarkGenerator_->OnEvent(pBatch, maxTimestampInBatch, onEventWatermarkOutput_);
            }
            delete vectorBatch;
        } else {
            reusingRecord_->replace(vectorBatch, maxTimestampInBatch);
            recordsOutput_->emitRecord(reusingRecord_);
            watermarkGenerator_->OnEvent(vectorBatch, maxTimestampInBatch, onEventWatermarkOutput_);
        }
    }

    ProcessingTimeService* timeService_ = nullptr;
    int64_t idleTimeoutMillis_ = 0;
    int64_t lastRecordTime_ = 0;
    bool isIdle_ = false;
    OmniDataOutputPtr recordsOutput_;
    WatermarkOutput* onEventWatermarkOutput_;
    WatermarkOutput* periodicWatermarkOutput_;
    TimestampAssigner* timestampAssigner_;
    WatermarkGenerator* watermarkGenerator_;
    StreamRecord* reusingRecord_;
    long autoWatermarkInterval_;
};
