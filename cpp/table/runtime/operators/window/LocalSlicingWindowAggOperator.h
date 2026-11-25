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

#ifndef FLINK_TNEL_LOCALSLICINGWINDOWAGGOPERATOR_H
#define FLINK_TNEL_LOCALSLICINGWINDOWAGGOPERATOR_H

#include <memory>
#include <regex>
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/WindowKey.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/watermark/Watermark.h"
#include "core/include/common.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/runtime/operators/aggregate/window/buffers/RecordsWindowBuffer.h"

class LocalSlicingWindowAggOperator : public AbstractStreamOperator<long>, public OneInputStreamOperator {
public:
    LocalSlicingWindowAggOperator(const nlohmann::json& config, Output* output)
        : AbstractStreamOperator(output),
          description(config)
    {
        inputTypes = config["inputTypes"].get<std::vector<std::string>>();
        outputTypeStr = config["outputTypes"].get<std::vector<std::string>>();
        clock = new ClockService();
        for (const auto& i : outputTypeStr) {
            outputTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
        }

        keyedIndex = config["grouping"].get<std::vector<int32_t>>();
        for (int32_t index : keyedIndex) {
            if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
                keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
            }
        }

        sliceAssigner = AssignerAtt::createSliceAssigner(description);
        if (description.contains("timeAttributeIndex")) {
            nlohmann::json rowtimeIndex = description["timeAttributeIndex"];
            rowtimeIndexVal = rowtimeIndex.get<long>();
        } else {
            rowtimeIndexVal = -1;
        }
        windowInterval = sliceAssigner->getSliceEndInterval();
        windowBuffer = std::make_unique<RecordsWindowBuffer>(description, output, sliceAssigner);
    }

    ~LocalSlicingWindowAggOperator() override
    {
        delete sliceAssigner;
    };
    void open() override;

    const char* getName() override;
    void close() override;
    void processBatch(StreamRecord* input) override;
    std::string getTypeName() override;

    void ProcessWatermark(Watermark* mark) override;
    void PrepareSnapshotPreBarrier(long checkpointId) override;
    void processElement(StreamRecord* element) override {};
    Output* getOutput();

    static long getNextTriggerWatermark(long watermark, long interval)
    {
        if (watermark == INT64_MAX) {
            return watermark;
        }
        long remainder = (interval <= 0) ? 0 : (watermark % interval);
        long start = remainder < 0L ? watermark - (remainder + interval) : watermark - remainder;
        long triggerWatermark = start + interval - 1L;

        return triggerWatermark > watermark ? triggerWatermark : triggerWatermark + interval;
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        // First do the shared initialization step
        INFO_RELEASE(
            "LocalSlicingWindowAggOperator initializeState with initializer, operatorID: "
            << OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<long>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<long>::initializeState(initializer, keySerializer);
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<long>::notifyCheckpointComplete(checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractStreamOperator<long>::notifyCheckpointAborted(checkpointId);
    }

private:
    std::unique_ptr<RecordsWindowBuffer> windowBuffer;
    nlohmann::json description;
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    int accumulatorArity = 0;
    std::vector<AggsHandleFunction*> functions;
    int aggregateCallsCount = 0;
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypeStr; // todo: remove from variables
    std::vector<omniruntime::type::DataTypeId> outputTypes;

    std::vector<int32_t> keyedTypes;
    std::vector<int32_t> keyedIndex;
    SliceAssigner* sliceAssigner = nullptr;
    long currentWatermark = 0;
    long nextTriggerWatermark = 0;
    long windowInterval = 0;

    int rowtimeIndexVal;
    ClockService* clock;
};

#endif
