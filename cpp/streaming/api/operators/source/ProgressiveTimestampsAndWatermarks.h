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

#ifndef OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H
#define OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H

#include <atomic>
#include <utility>
#include "TimestampsAndWatermarks.h"
#include "core/api/common/eventtime/WatermarkOutputMultiplexer.h"
#include "SourceOutputWithWatermarks.h"
#include "runtime/tasks/ScheduledFutureTask.h"

class ProgressiveTimestampsAndWatermarks : public TimestampsAndWatermarks,
                                           std::enable_shared_from_this<ProgressiveTimestampsAndWatermarks> {
public:
    ProgressiveTimestampsAndWatermarks(
        TimestampAssigner* timestampAssigner,
        std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory,
        ProcessingTimeService* timeService,
        long periodicWatermarkInterval,
        int64_t idleTimeoutMillis = 0);

    class IdlenessManager {
    public:
        // 内部 IdlenessAwareWatermarkOutput 类
        class IdlenessAwareWatermarkOutput : public WatermarkOutput {
        public:
            IdlenessAwareWatermarkOutput(WatermarkOutput* output, IdlenessManager* parent)
                : underlyingOutput_(output),
                  isIdle_(true),
                  parentManager_(parent)
            {
            }

            ~IdlenessAwareWatermarkOutput() override = default;
            void emitWatermark(Watermark* watermark) override
            {
                underlyingOutput_->emitWatermark(watermark);
                isIdle_ = false;
            }

            void MarkIdle() override
            {
                isIdle_ = true;
                parentManager_->CheckAndMarkIdle();
            }

            void MarkActive() override
            {
                isIdle_ = false;
                underlyingOutput_->MarkActive();
            }

            [[nodiscard]] bool IsIdle() const
            {
                return isIdle_;
            }

        private:
            WatermarkOutput* underlyingOutput_;
            bool isIdle_;
            IdlenessManager* parentManager_; // 指向父管理器的指针
        };

        explicit IdlenessManager(WatermarkOutput* output)
            : underlyingOutput_(output),
              splitLocalOutput_(new IdlenessAwareWatermarkOutput(output, this)),
              mainOutput_(new IdlenessAwareWatermarkOutput(output, this))
        {
        }

        ~IdlenessManager()
        {
            delete underlyingOutput_;
            delete splitLocalOutput_;
            delete mainOutput_;
        }

        IdlenessAwareWatermarkOutput* GetSplitLocalOutput()
        {
            return splitLocalOutput_;
        }

        IdlenessAwareWatermarkOutput* GetMainOutput()
        {
            return mainOutput_;
        }

    private:
        WatermarkOutput* underlyingOutput_;
        IdlenessAwareWatermarkOutput* splitLocalOutput_;
        IdlenessAwareWatermarkOutput* mainOutput_;

        void CheckAndMarkIdle()
        {
            if (splitLocalOutput_->IsIdle() && mainOutput_->IsIdle()) {
                underlyingOutput_->MarkIdle();
            }
        }
    };

    class SplitLocalOutputs {
    public:
        SplitLocalOutputs(
            OmniDataOutputPtr recordOutput,
            WatermarkOutput* watermarkOutput,
            TimestampAssigner* timestampAssigner,
            std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory,
            long periodicWatermarkInterval)
            : recordOutput(recordOutput),
              timestampAssigner(timestampAssigner),
              watermarksFactory(watermarksFactory),
              periodicWatermarkInterval_(periodicWatermarkInterval)
        {
            watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
        }

        ~SplitLocalOutputs()
        {
            delete watermarkMultiplexer;
            for (auto& pair : localOutputs) {
                delete pair.second;
            }
        }

        // 禁止拷贝
        SplitLocalOutputs(const SplitLocalOutputs&) = delete;
        SplitLocalOutputs& operator=(const SplitLocalOutputs&) = delete;

        // 允许移动
        SplitLocalOutputs(SplitLocalOutputs&&) = default;
        SplitLocalOutputs& operator=(SplitLocalOutputs&&) = default;

        ProcessingTimeService* timeService = nullptr;
        int64_t idleTimeoutMillis = 0;

        SourceOutputWithWatermarks* CreateOutputForSplit(const std::string& splitId)
        {
            auto it = localOutputs.find(splitId);
            if (it != localOutputs.end()) {
                return it->second;
            }

            watermarkMultiplexer->RegisterNewOutput(splitId);
            WatermarkOutput* onEventOutput = watermarkMultiplexer->GetImmediateOutput(splitId);
            WatermarkOutput* periodicOutput = watermarkMultiplexer->GetDeferredOutput(splitId);

            auto watermarks = watermarksFactory->CreateWatermarkGenerator();

            auto localOutput = SourceOutputWithWatermarks::createWithSeparateOutputs(
                recordOutput, onEventOutput, periodicOutput, timestampAssigner, watermarks, periodicWatermarkInterval_);

            localOutput->ConfigureIdleness(timeService, idleTimeoutMillis);
            localOutputs.emplace(splitId, localOutput);
            return localOutput;
        }

        void ReleaseOutputForSplit(const std::string& splitId)
        {
            auto it = localOutputs.find(splitId);
            if (it == localOutputs.end()) {
                return;
            }
            SourceOutputWithWatermarks* value = it->second;
            localOutputs.erase(it);
            watermarkMultiplexer->UnregisterOutput(splitId);
            delete value;
        }

        void EmitPeriodicWatermark()
        {
            for (auto& pair : localOutputs) {
                pair.second->EmitPeriodicWatermark();
            }
            watermarkMultiplexer->OnPeriodicEmit();
        }

    private:
        WatermarkOutputMultiplexer* watermarkMultiplexer = nullptr;
        std::unordered_map<std::string, SourceOutputWithWatermarks*> localOutputs;
        OmniDataOutputPtr recordOutput;
        TimestampAssigner* timestampAssigner;
        std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory;
        long periodicWatermarkInterval_;
    };

    class StreamingReaderOutput : public SourceOutputWithWatermarks {
    public:
        StreamingReaderOutput(
            OmniDataOutputPtr output,
            WatermarkOutput* watermarkOutput,
            TimestampAssigner* timestampAssigner,
            WatermarkGenerator* watermarkGenerator,
            SplitLocalOutputs* splitLocalOutputs,
            long periodicWatermarkInterval)
            : SourceOutputWithWatermarks(
                  output,
                  watermarkOutput,
                  watermarkOutput,
                  timestampAssigner,
                  watermarkGenerator,
                  periodicWatermarkInterval),
              splitLocalOutputs(splitLocalOutputs)
        {
        }

        SourceOutput& CreateOutputForSplit(const std::string& splitId) override
        {
            return *(splitLocalOutputs->CreateOutputForSplit(splitId));
        }

        void ReleaseOutputForSplit(const std::string& splitId) override
        {
            splitLocalOutputs->ReleaseOutputForSplit(splitId);
        }

    private:
        SplitLocalOutputs* splitLocalOutputs;
    };

    class TriggerProcessingTimeCallback : public ProcessingTimeCallback {
    public:
        explicit TriggerProcessingTimeCallback(std::shared_ptr<ProgressiveTimestampsAndWatermarks> eventTimeLogic)
            : eventTimeLogic(eventTimeLogic)
        {
        }

        ~TriggerProcessingTimeCallback() override = default;

        void OnProcessingTime(int64_t timestamp) override
        {
            auto logic = eventTimeLogic.lock();
            if (logic == nullptr) {
                return;
            }
            logic->TriggerPeriodicEmit(timestamp);
        }

    private:
        std::weak_ptr<ProgressiveTimestampsAndWatermarks> eventTimeLogic;
    };

    std::shared_ptr<TriggerProcessingTimeCallback> callback;

    ~ProgressiveTimestampsAndWatermarks() override
    {
        StopPeriodicWatermarkEmits();
        delete timestampAssigner;
        delete currentPerSplitOutputs;
        delete idlenessManager;
        // The scheduler stores raw callback pointers; the weak callback is left alive so queued tasks can no-op.
    }

    ReaderOutput* CreateMainOutput(OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback) override;
    void StartPeriodicWatermarkEmits() override;
    void StopPeriodicWatermarkEmits() override;

private:
    TimestampAssigner* timestampAssigner = nullptr;
    std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory;
    ProcessingTimeService* timeService = nullptr;
    int64_t idleTimeoutMillis_;
    long periodicWatermarkInterval;

    std::weak_ptr<ScheduledFutureTask> periodicEmitHandle;
    std::atomic<bool> periodicEmitStopped_ = true;

    void TriggerPeriodicEmit(long wallClockTimestamp);

    SplitLocalOutputs* currentPerSplitOutputs = nullptr;
    StreamingReaderOutput* currentMainOutput = nullptr;
    IdlenessManager* idlenessManager = nullptr;
};

#endif // OMNISTREAM_PROGRESSIVETIMESTAMPSANDWATERMARKS_H
