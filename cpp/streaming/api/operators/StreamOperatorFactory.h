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

#ifndef FLINK_TNEL_STREAMOPERATORFACTORY_H
#define FLINK_TNEL_STREAMOPERATORFACTORY_H

#include <executiongraph/operatorchain/OperatorPOD.h>
#include <executiongraph/operatorchain/OperatorPOD.h>
#include "OneInputStreamOperator.h"
#include "../../../core/graph/OperatorConfig.h"
#include "streaming/runtime/io/RecordWriterOutput.h"
#include "StreamOperator.h"

namespace omnistream {
class OmniStreamTask;
class StreamOperatorFactory {
public:
    static StreamOperator* createOperatorAndCollector(
        omnistream::OperatorConfig& opConfig, WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* createOperatorAndCollector(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<OmniStreamTask> task);

private:
    static StreamOperator* CreateStreamCalcOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamJoinOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateLocalWindowAggOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateGlobalWindowAggOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateGroupWindowAggOp(
        OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateWatermarkAssignerOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateKeyedProcessOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSinkOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSourceOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamExpandOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateTimestampInserterOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateProcessOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateConstraintEnforcerOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateWindowJoinOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateWindowTableFunctionOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamingFileWriterOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreatePartitionCommitterOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateInputConversionOperator(
        omnistream::OperatorPOD& opDesc,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateFilterOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateMapOp(omnistream::OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateBatchFilterOp(
        omnistream::OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateFlatMapOp(
        omnistream::OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateReduceOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSinkWriterOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task,
        ProcessingTimeService* processingTimeService);

    static StreamOperator* CreateCommitOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task,
        ProcessingTimeService* processingTimeService);

    static StreamOperator* CreateKeyedCoProcessOp(
        omnistream::OperatorPOD& opConfig,
        WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
};
} // namespace omnistream

#endif // FLINK_TNEL_STREAMOPERATORFACTORY_H
