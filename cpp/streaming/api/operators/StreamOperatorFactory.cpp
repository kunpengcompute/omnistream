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

#include <stdexcept>
#include <iostream>
#include <regex>
#include <streaming/runtime/tasks/omni/OmniStreamTask.h>
#include <utility>
#include "operatorconstants.h"
#include "StreamFlatMap.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "StreamCalcBatch.h"
#include "StreamExpand.h"
#include "StreamSource.h"
#include "StreamMap.h"
#include "StreamFilter.h"
#include "StreamGroupedReduceOperator.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "table/runtime/operators/deduplicate/ProcTimeDeduplicateKeepLastRowFunction.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/join/stream/StreamingJoinOperator.h"
#include "table/runtime/operators/join/stream/StreamingSemiAntiJoinOperator.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "table/runtime/operators/wmassigners/WatermarkAssignerOperator.h"
#include "table/runtime/operators/sink/SinkOperator.h"
#include "table/runtime/operators/sink/TimeStampInserterSinkOperator.h"
#include "streaming/api/operators/ProcessOperator.h"
#include "streaming/api/functions/sink/filesystem/StreamingFileWriter.h"
#include "streaming/api/functions/sink/filesystem/PartitionCommitter.h"
#include "runtime/operators/join/lookup/LookupJoinRunner.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "table/runtime/operators/source/ValuesSourceFunction.h"
#include "table/runtime/operators/source/csv/CsvInputFormat.h"
#include "runtime/operators/window/LocalSlicingWindowAggOperator.h"
#include "streaming/runtime/tasks/ChainingOutput.h"
#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "runtime/operators/sink/ConstraintEnforcer.h"
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/runtime/operators/rank/RetractableTopNFunction.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "table/runtime/operators/rank/FastTop1Function.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
#include "table/runtime/operators/join/window/LeftOuterJoinOperator.h"
#include "table/runtime/operators/join/window/RightOuterJoinOperator.h"
#include "table/runtime/operators/join/window/FullOuterJoinOperator.h"
#include "runtime/operators/window/AggregateWindowOperator.h"
#include "runtime/operators/window/tvf/WindowTableFunctionOperator.h"
#include "datagen/meituan/JoinSource.h"
#include "datagen/meituan/MTFilterFunc.h"
#include "connector/kafka/source/KafkaSource.h"
#include "api/operators/SourceOperator.h"
#include "connector/kafka/sink/DeliveryGuarantee.h"
#include "connector/kafka/sink/KafkaSink.h"
#include "streaming/api/operators/sink/SinkWriterOperator.h"
#include "streaming/api/operators/sink/CommitterOperator.h"
#include "connector/kafka/sink/ProducerConfig.h"
#include "connector/kafka/utils/ConfigLoader.h"
#include "co/KeyedCoProcessOperator.h"
#include "StreamOperatorFactory.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/checkpoint/WindowAggSavepointCompatibility.h"

namespace omnistream {

namespace {

template <typename OperatorT>
void configureSavepoint(
    OperatorT* op,
    const char* operatorType,
    const nlohmann::json& description,
    const FlinkSavepointAdaptorInfo& adaptorInfo)
{
    op->setDescription(description);
    if (adaptorInfo.type == FlinkSavepointAdaptorType::None) {
        op->setFlinkSavepointUnsupported(adaptorInfo.reason);
    } else {
        op->setFlinkSavepointAdaptor(adaptorInfo.type);
    }
    INFO_RELEASE(
        "StreamOperatorFactory configured compatible savepoint adaptor: operatorType="
        << operatorType << ", adaptorType=" << flinkSavepointAdaptorTypeName(adaptorInfo.type)
        << ", adaptorTypeCode=" << static_cast<int>(adaptorInfo.type)
        << ", reason=" << (adaptorInfo.reason.empty() ? "<none>" : adaptorInfo.reason));
}

} // namespace

StreamOperator* StreamOperatorFactory::createOperatorAndCollector(
    omnistream::OperatorConfig& opConfig, WatermarkGaugeExposingOutput* chainOutput)
{
    // NOT_IMPL_EXCEPTION
    auto uniqueName = opConfig.getUniqueName();
    LOG("getUniqueName :" + uniqueName);

    if (uniqueName == OPERATOR_NAME_STREAM_EXPAND) {
        auto* execExpand = new StreamExpand(opConfig.getDescription(), chainOutput);
        execExpand->setup();
        LOG("Operator StreamExpand address  " + std::to_string(reinterpret_cast<long>(execExpand)));
        return static_cast<OneInputStreamOperator*>(execExpand);
    } else if (uniqueName == OPERATOR_NAME_STREAM_CALC) {
        auto* execCalc = new StreamCalcBatch(opConfig.getDescription(), chainOutput);
        execCalc->setup();
        LOG("Operator StreamCalcBatch address  " + std::to_string(reinterpret_cast<long>(execCalc)));
        return static_cast<OneInputStreamOperator*>(execCalc);
    } else if (uniqueName == OPERATOR_NAME_STREAM_JOIN || uniqueName == OPERATOR_NAME_STREAM_SEMI_ANTI_JOIN) {
        const auto& desc = opConfig.getDescription();
        std::string joinType = desc.contains("joinType") ? desc["joinType"].get<std::string>() : "InnerJoin";
        if (joinType == "LeftSemiJoin" || joinType == "LeftAntiJoin") {
            LOG("Generating StreamingSemiAntiJoinOperator...");
            auto* op = new StreamingSemiAntiJoinOperator<RowData*>(desc, chainOutput);
            op->setup();
            return static_cast<TwoInputStreamOperator*>(op);
        }
        LOG("Generating StreamingJoinOperator...");
        auto opDescriptionJSON = opConfig.getDescription();
        auto* op = new StreamingJoinOperator<RowData*>(opDescriptionJSON, chainOutput);
        op->setup();
        op->setDescription(opDescriptionJSON);
        const FlinkSavepointAdaptorType adaptorType =
            omnistream::StreamingJoinSavepointUtil::getAdaptorType(opDescriptionJSON);
        if (adaptorType == FlinkSavepointAdaptorType::None) {
            op->setFlinkSavepointUnsupported(
                omnistream::StreamingJoinSavepointUtil::buildUnsupportedReason(opDescriptionJSON));
        } else {
            op->setFlinkSavepointAdaptor(adaptorType);
        }
        return static_cast<TwoInputStreamOperator*>(op);
    } else if (uniqueName == OPERATOR_NAME_WATERMARK_ASSIGNER) {
        auto* watermarkAssignerOperator = new WatermarkAssignerOperator(
            chainOutput,
            opConfig.getDescription()["rowtimeFieldIndex"],
            4000,
            opConfig.getDescription()["idleTimeout"],
            nullptr);
        watermarkAssignerOperator->setup();
        return static_cast<OneInputStreamOperator*>(watermarkAssignerOperator);
    } else if (uniqueName == OPERATOR_NAME_KEYED_PROCESS_OPERATOR) {
        if (opConfig.getDescription().value("isRowtime", false) ||
            (!opConfig.getName().empty() && opConfig.getName()[0] == 'D')) {
            RowTimeDeduplicateFunction* func = new RowTimeDeduplicateFunction(opConfig.getDescription());
            auto* op = new KeyedProcessOperator(func, chainOutput, opConfig.getDescription());
            op->setup();
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));

            return static_cast<OneInputStreamOperator*>(op);
        } else {
            GroupAggFunction* func = new GroupAggFunction(0l, opConfig.getDescription());
            auto* op = new KeyedProcessOperator(func, chainOutput, opConfig.getDescription());
            op->setup();
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));

            return static_cast<OneInputStreamOperator*>(op);
        }
    } else if (uniqueName == OPERATOR_NAME_SINK || uniqueName == OPERATOR_NAME_COLLECT_SINK) {
        nlohmann::json object;
        if (opConfig.getName() != "") {
            object["outputfile"] = "/tmp/" + opConfig.getName() + ".txt";
        } else {
            object["outputfile"] = "/tmp/sink.txt";
        }

        return static_cast<OneInputStreamOperator*>(new SinkOperator(object));
    } else if (uniqueName == OPERATOR_NAME_PROCESS_OPERATOR) {
        const nlohmann::json& description = opConfig.getDescription();
        // jobType: the task contains one or more tasks, and the type must be either stream, sql, or sql_stream.
        // taskType: the task contains one or more operators, and the type must be either stream, sql, or sql_stream.
        // operatorType: the operator type must be either stream or sql.
        if (description["operatorType"] == "stream") {
            ProcessOperator<Object*, Object*>* op;
            op = new ProcessOperator<Object*, Object*>(description, chainOutput, true);
            op->setup();
            LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<OneInputStreamOperator*>(op);
        } else {
            // description["operatorType"] == "sql"
            LookupJoinRunner* runner = new LookupJoinRunner(description, chainOutput);
            auto op = new ProcessOperator<omnistream::VectorBatch*, omnistream::VectorBatch*>(
                runner, description, chainOutput, false);
            op->setup();
            LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<OneInputStreamOperator*>(op);
        }
    } else if (uniqueName == OPERATOR_NAME_LOCAL_WINDOW_AGG) {
        const auto description = opConfig.getDescription();
        auto* op = new LocalSlicingWindowAggOperator(description, chainOutput);
        op->setup();
        configureSavepoint(op, "LocalWindowAgg", description, WindowAggSavepointCompatibility::forLocal());
        LOG("Operator LocalSlicingWindowAggOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (uniqueName == OPERATOR_NAME_GLOBAL_WINDOW_AGG) {
        const auto description = opConfig.getDescription();
        auto processor = std::make_unique<AbstractWindowAggProcessor>(description, chainOutput);
        auto* op = new SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>(std::move(processor), description);
        op->setup();
        configureSavepoint(
            op, "SlicingWindowAgg", description, WindowAggSavepointCompatibility::forSlicing(description));
        LOG("Operator SlicingWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (opConfig.getUniqueName() == OPERATOR_NAME_GROUP_WINDOW_AGG) {
        const auto description = opConfig.getDescription();
        auto* op = new AggregateWindowOperator<std::shared_ptr<RowData>, TimeWindow>(description, chainOutput);
        op->setup();
        configureSavepoint(op, "GroupWindowAgg", description, WindowAggSavepointCompatibility::forGroup(description));
        LOG("Operator AggregateWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (opConfig.getUniqueName() == OPERATOR_NAME_WINDOW_TABLE_FUNCTION) {
        const auto description = opConfig.getDescription();
        auto* op = new WindowTableFunctionOperator(description, chainOutput);
        // WindowTableFunction supports processing-time windows, which need a
        // ProcessingTimeService. This overload has no task to provide one, so fall
        // back to a standalone service (same as the no-task kafka source branch).
        op->setProcessingTimeService(new SystemProcessingTimeService());
        op->setup();
        op->setDescription(description);
        op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
        LOG("Operator WindowTableFunctionOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (
        uniqueName == OPERATOR_NAME_WINDOW_INNER_JOIN || uniqueName == OPERATOR_NAME_WINDOW_LEFT_OUTER_JOIN ||
        uniqueName == OPERATOR_NAME_WINDOW_RIGHT_OUTER_JOIN || uniqueName == OPERATOR_NAME_WINDOW_FULL_OUTER_JOIN) {
        auto opDescriptionJSON = opConfig.getDescription();
        std::string joinType =
            opDescriptionJSON.contains("joinType") ? opDescriptionJSON["joinType"].get<std::string>() : "InnerJoin";
        WindowJoinOperator<std::shared_ptr<RowData>>* op = nullptr;
        if (joinType == "LeftOuterJoin") {
            op = new LeftOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
        } else if (joinType == "RightOuterJoin") {
            op = new RightOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
        } else if (joinType == "FullOuterJoin") {
            op = new FullOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
        } else {
            op = new InnerJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
        }
        op->setup();
        LOG("Operator WindowJoinOperator address " + std::to_string(reinterpret_cast<long>(op)));

        // SP-INTEROP: 启用 WindowJoin 兼容适配器
        const FlinkSavepointAdaptorType adaptorType =
            omnistream::StreamingJoinSavepointUtil::getWindowJoinAdaptorType(opDescriptionJSON);
        if (adaptorType == FlinkSavepointAdaptorType::None) {
            op->setFlinkSavepointUnsupported(
                omnistream::StreamingJoinSavepointUtil::buildWindowJoinUnsupportedReason(opDescriptionJSON));
        } else {
            op->setDescription(opDescriptionJSON);
            op->setFlinkSavepointAdaptor(adaptorType);
        }
        return static_cast<TwoInputStreamOperator*>(op);
    } else {
        THROW_LOGIC_EXCEPTION("Unknown operator " + uniqueName);
    }
}
/**
 * std::shared_ptr<OmniStreamTask> task is ressareve for futur.
 **/

StreamOperator* StreamOperatorFactory::createOperatorAndCollector(
    omnistream::OperatorPOD& opDesc, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<OmniStreamTask> task)
{
    auto operatorID = opDesc.getId();
    INFO_RELEASE("StreamOperatorFactory operatorID  :" << operatorID);

    if (operatorID == OPERATOR_NAME_STREAM_CALC) {
        return CreateStreamCalcOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_STREAM_JOIN) {
        return CreateStreamJoinOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_STREAM_SEMI_ANTI_JOIN) {
        return CreateStreamJoinOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_LOCAL_WINDOW_AGG) {
        return CreateLocalWindowAggOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_GLOBAL_WINDOW_AGG) {
        return CreateGlobalWindowAggOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_GROUP_WINDOW_AGG) {
        return CreateGroupWindowAggOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_WATERMARK_ASSIGNER) {
        // TODO: Idle timeout is currently set to 0, but might change depending on the config
        return CreateWatermarkAssignerOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_KEYED_PROCESS_OPERATOR) {
        return CreateKeyedProcessOp(opDesc, chainOutput, task);
    } else if (
        operatorID == OPERATOR_NAME_SINK || operatorID == OPERATOR_NAME_COLLECT_SINK ||
        operatorID == OPERATOR_NAME_STREAM_SINK) {
        return CreateSinkOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_STREAM_SOURCE || operatorID == OPERATOR_NAME_SOURCE_OPERATOR) {
        return CreateSourceOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_STREAM_EXPAND) {
        return CreateStreamExpandOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_STREAMRECORDTIMESTAMPINSERTER) {
        return CreateTimestampInserterOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_PROCESS_OPERATOR) {
        return CreateProcessOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_CONSTRAINTENFORCER) {
        return CreateConstraintEnforcerOp(opDesc, chainOutput, task);
    } else if (
        operatorID == OPERATOR_NAME_WINDOW_INNER_JOIN || operatorID == OPERATOR_NAME_WINDOW_LEFT_OUTER_JOIN ||
        operatorID == OPERATOR_NAME_WINDOW_RIGHT_OUTER_JOIN || operatorID == OPERATOR_NAME_WINDOW_FULL_OUTER_JOIN) {
        return CreateWindowJoinOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_WINDOW_TABLE_FUNCTION) {
        return CreateWindowTableFunctionOp(opDesc, chainOutput, task);
    } else if (operatorID == datastream::OPERATOR_NAME_FLATMAP) {
        return CreateFlatMapOp(opDesc, chainOutput);
    } else if (operatorID == datastream::OPERATOR_NAME_MAP) {
        return CreateMapOp(opDesc, chainOutput);
    } else if (operatorID == datastream::OPERATOR_NAME_REDUCE) {
        return CreateReduceOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_FILTER) {
        return CreateFilterOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_SINK_WRITER) {
        auto processingTimeService = task->createProcessingTimeService();
        return CreateSinkWriterOp(opDesc, chainOutput, task, processingTimeService);
    } else if (operatorID == OPERATOR_NAME_COMMIT_OPERATOR) {
        auto processingTimeService = task->createProcessingTimeService();
        return CreateCommitOp(opDesc, chainOutput, task, processingTimeService);
    } else if (operatorID == OPERATOR_NAME_STREAMING_FILE_WRITER) {
        return CreateStreamingFileWriterOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_PARTITION_COMMITTER) {
        return CreatePartitionCommitterOp(opDesc, chainOutput, task);
    } else if (operatorID == OPERATOR_NAME_KEYED_CO_PROCESS) {
        return CreateKeyedCoProcessOp(opDesc, chainOutput, task);
    } else {
        return nullptr;
    }
}

StreamOperator* StreamOperatorFactory::CreateStreamCalcOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto* execCalc = new StreamCalcBatch(opDescriptionJSON, chainOutput);
    execCalc->setup(std::move(task));
    execCalc->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);

    LOG("Operator StreamCalc address  " + std::to_string(reinterpret_cast<long>(execCalc)));
    return static_cast<OneInputStreamOperator*>(execCalc);
}

StreamOperator* StreamOperatorFactory::CreateStreamJoinOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    // Route semi/anti (EXISTS/NOT EXISTS) to StreamingSemiAntiJoinOperator by joinType.
    std::string joinType =
        opDescriptionJSON.contains("joinType") ? opDescriptionJSON["joinType"].get<std::string>() : "InnerJoin";
    if (joinType == "LeftSemiJoin" || joinType == "LeftAntiJoin") {
        LOG("Generating StreamingSemiAntiJoinOperator...");
        auto* op = new StreamingSemiAntiJoinOperator<RowData*>(opDescriptionJSON, chainOutput);
        op->setup(std::move(task));
        return static_cast<TwoInputStreamOperator*>(op);
    }
    LOG("Generating StreamingJoinOperator...");
    auto* op = new StreamingJoinOperator<RowData*>(opDescriptionJSON, chainOutput);
    op->setup(std::move(task));
    // SP-INTEROP: StreamingJoin 的 compatible Adaptor 按当前真实算子 description 分类，
    // 范围外 Join 仍设置 None + reason，在 compatible 保存/恢复时 fail fast，避免静默产出空/错误 SP。
    op->setDescription(opDescriptionJSON);
    const FlinkSavepointAdaptorType adaptorType =
        omnistream::StreamingJoinSavepointUtil::getAdaptorType(opDescriptionJSON);
    if (adaptorType == FlinkSavepointAdaptorType::None) {
        op->setFlinkSavepointUnsupported(
            omnistream::StreamingJoinSavepointUtil::buildUnsupportedReason(opDescriptionJSON));
    } else {
        op->setFlinkSavepointAdaptor(adaptorType);
    }
    return static_cast<TwoInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateLocalWindowAggOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto* op = new LocalSlicingWindowAggOperator(opDescriptionJSON, chainOutput);
    op->setup(std::move(task));
    configureSavepoint(op, "LocalWindowAgg", opDescriptionJSON, WindowAggSavepointCompatibility::forLocal());
    LOG("Operator LocalSlicingWindowAggOperator address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateGlobalWindowAggOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto processor = std::make_unique<AbstractWindowAggProcessor>(opDescriptionJSON, chainOutput);
    auto* op = new SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>(std::move(processor), opDescriptionJSON);
    auto processingTimeService = task->createProcessingTimeService();
    op->setProcessingTimeService(processingTimeService);
    op->setup(std::move(task));
    configureSavepoint(
        op, "SlicingWindowAgg", opDescriptionJSON, WindowAggSavepointCompatibility::forSlicing(opDescriptionJSON));
    LOG("Operator SlicingWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateGroupWindowAggOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto* op = new AggregateWindowOperator<std::shared_ptr<RowData>, TimeWindow>(opDescriptionJSON, chainOutput);
    auto processingTimeService = task->createProcessingTimeService();
    op->setProcessingTimeService(processingTimeService);
    op->setup(std::move(task));
    configureSavepoint(
        op, "GroupWindowAgg", opDescriptionJSON, WindowAggSavepointCompatibility::forGroup(opDescriptionJSON));
    LOG("Operator AggregateWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateWatermarkAssignerOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto processingTimeService = task->createProcessingTimeService();
    auto* watermarkAssignerOperator = new WatermarkAssignerOperator(
        chainOutput,
        opDescriptionJSON["rowtimeFieldIndex"],
        opDescriptionJSON["intervalSecond"],
        0,
        processingTimeService);
    bool splitWaterMark = task->env()->taskConfiguration().GetSplitWatermark();
    watermarkAssignerOperator->setSplitWaterMark(splitWaterMark);
    INFO_RELEASE("should do splitWaterMark : " << splitWaterMark);
    // SP-INTEROP: WatermarkAssigner 为无 managed state 的辅助节点，设 OmniIsCompatible。
    watermarkAssignerOperator->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    return static_cast<OneInputStreamOperator*>(watermarkAssignerOperator);
}

StreamOperator* StreamOperatorFactory::CreateKeyedProcessOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    LOG("opDesc.getName: " << opConfig.getName());
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);

    if (opDescriptionJSON.value("processFunction", "") == "ProcTimeDeduplicateKeepLastRowFunction") {
        const bool inputInsertOnly = opDescriptionJSON.value("inputInsertOnly", false);
        auto* function = new ProcTimeDeduplicateKeepLastRowFunction(opDescriptionJSON);
        auto* op = new KeyedProcessOperator(function, chainOutput, opDescriptionJSON);
        op->setup(std::move(task));
        op->setDescription(opDescriptionJSON);
        // 路由隔离：ChangelogNormalize 与 append-only proc-time Deduplicate 在 Flink 中复用同一
        // ProcTimeDeduplicateKeepLastRowFunction，仅以 inputInsertOnly 区分。这里据该标志分离，
        // 避免仅按函数名路由把 Deduplicate 误当 ChangelogNormalize。
        if (inputInsertOnly) {
            op->setFlinkSavepointUnsupported("proc-time Deduplicate savepoints are unsupported");
            LOG("Routed ProcTimeDeduplicateKeepLastRowFunction -> proc-time Deduplicate (inputInsertOnly=true)");
        } else {
            op->setFlinkSavepointUnsupported("ChangelogNormalize savepoints are unsupported");
            LOG("Routed ProcTimeDeduplicateKeepLastRowFunction -> ChangelogNormalize (inputInsertOnly=false)");
        }
        return static_cast<OneInputStreamOperator*>(op);
    }
    if (opDescriptionJSON.value("isRowtime", false) ||
        (!opConfig.getName().empty() && opConfig.getName()[0] == 'D')) {
        LOG("Deduplicate description: " << opConfig.getDescription());
        RowTimeDeduplicateFunction* func = new RowTimeDeduplicateFunction(opDescriptionJSON);
        LOG("Deduplicate2");
        auto* op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
        LOG("Deduplicate3");
        op->setup(std::move(task));
        op->setDescription(opDescriptionJSON);
        op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::DeduplicateAdaptor);
        LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (
        opDescriptionJSON.contains("processFunction") &&
        opDescriptionJSON["processFunction"] == "AppendOnlyTopNFunction") {
        LOG("AppendOnlyTopNFunction description: " << opConfig.getDescription());
        AbstractTopNFunction<RowData*>* func = new AppendOnlyTopNFunction<RowData*>(opDescriptionJSON);
        auto* op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
        op->setup(std::move(task));
        if (opDescriptionJSON.contains("sortFieldIndices") && opDescriptionJSON["sortFieldIndices"].empty()) {
            // SP-INTEROP: streaming Limit shape (empty sort key). The AppendOnlyTopN
            // savepoint adaptor is not audited for the arity-0 sort key group, so mark
            // unsupported instead.
            op->setFlinkSavepointUnsupported(
                "Streaming Limit (empty sort key) native state is not convertible by the "
                "AppendOnlyTopN savepoint adaptor, which requires at least one sort column");
        } else {
            op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::AppendOnlyTopNAdaptor);
        }
        op->setDescription(opDescriptionJSON);
        LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (
        opDescriptionJSON.contains("processFunction") &&
        opDescriptionJSON["processFunction"] == "RetractableTopNFunction") {
        // Streaming Limit retract path (input contains updates/deletes).
        LOG("RetractableTopNFunction description: " << opConfig.getDescription());
        AbstractTopNFunction<RowData*>* func = new RetractableTopNFunction<RowData*>(opDescriptionJSON);
        auto* op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
        op->setup(std::move(task));
        // SP-INTEROP: no savepoint adaptor converts the retract state yet.
        op->setFlinkSavepointUnsupported(
            "Streaming Limit retract state is not convertible by any existing savepoint adaptor");
        op->setDescription(opDescriptionJSON);
        LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (
        opDescriptionJSON.contains("processFunction") && opDescriptionJSON["processFunction"] == "FastTop1Function") {
        AbstractTopNFunction<RowData*>* func = new FastTop1Function<RowData*>(opDescriptionJSON);
        auto* op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
        op->setup(std::move(task));
        op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
        op->setDescription(opDescriptionJSON);
        LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else {
        LOG("KeyedProcessOperator has no matching processFunction/isRowtime; falling back to GroupAggFunction. description: "
            << opConfig.getDescription());
        GroupAggFunction* func = new GroupAggFunction(0l, opDescriptionJSON);
        auto* op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
        op->setup(std::move(task));
        // GroupAggFunction 在初始化时会过滤掉包含 "RAW" 的 accType（对应基于 DataView 的累加器）。
        // 仅当原始 accTypes 中存在 RAW 类型时，才需要 GroupAggAdaptor 来处理 RAW 占位与
        // DataView keyed state 的 savepoint 转换；否则 Omni 原生格式即可与 Flink 兼容。
        const auto& accTypes = opDescriptionJSON["aggInfoList"]["accTypes"];
        bool hasRawAccType = false;
        for (const auto& type : accTypes) {
            if (type.get<std::string>().find("RAW") != std::string::npos) {
                hasRawAccType = true;
                break;
            }
        }
        op->setFlinkSavepointAdaptor(
            hasRawAccType ? FlinkSavepointAdaptorType::GroupAggAdaptor : FlinkSavepointAdaptorType::OmniIsCompatible);
        op->setDescription(opDescriptionJSON);
        LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    }
    return nullptr;
}

StreamOperator* StreamOperatorFactory::CreateSinkOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    const char* env = std::getenv("WRITE_TO_FILE");
    opDescriptionJSON["outputfile"] = (env && std::string(env) == "TRUE") ? std::string("/tmp/flink_output.txt") : "";
    auto* op = new SinkOperator(opDescriptionJSON);
    // SP-INTEROP: SinkOperator 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateSourceOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::string format = opDescriptionJSON["format"];
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        if (format == "kafka") {
            auto description = opConfig.getDescription();
            nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
            // create kafka source
            std::shared_ptr<KafkaSource> source = std::make_shared<KafkaSource>(opDescriptionJSON, false);
            ProcessingTimeService* timeService =
                task != nullptr ? task->createProcessingTimeService() : new SystemProcessingTimeService();
            auto* op = new SourceOperator(chainOutput, opDescriptionJSON, source, timeService);
            op->setup(std::move(task));
            LOG("Operator SourceOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return op;
        } else {
            auto* source = new omnistream::StreamSource<Object>(chainOutput, opDescriptionJSON, true);
            source->setup(std::move(task));
            return static_cast<StreamOperator*>(source);
        }
    } else if (operatorType == Type_o::SQL) {
        // operatorType == "sql"
        if (format == "csv") {
            std::vector<DataTypeId> fields;
            std::vector<int32_t> scales;
            std::vector<int32_t> precisions;
            std::regex decimalPattern(R"(DECIMAL\d*\((\d+),\s*(\d+)\))");
            std::smatch match;
            for (auto& field : opDescriptionJSON["fields"]) {
                std::string typeStr = field["type"];
                DataTypeId typeId = LogicalType::flinkTypeToOmniTypeId(typeStr);
                fields.push_back(typeId);
                if (std::regex_search(typeStr, match, decimalPattern)) {
                    precisions.push_back(std::stoi(match[1].str()));
                    scales.push_back(std::stoi(match[2].str()));
                } else if (typeId == DataTypeId::OMNI_DECIMAL64) {
                    precisions.push_back(omniruntime::type::DECIMAL64_DEFAULT_PRECISION);
                    scales.push_back(omniruntime::type::DECIMAL64_DEFAULT_SCALE);
                } else if (typeId == DataTypeId::OMNI_DECIMAL128) {
                    precisions.push_back(omniruntime::type::DECIMAL128_DEFAULT_PRECISION);
                    scales.push_back(omniruntime::type::DECIMAL128_DEFAULT_SCALE);
                } else {
                    precisions.push_back(0);
                    scales.push_back(0);
                }
            }
            omnistream::csv::CsvSchema schema(fields);
            schema.setScales(scales);
            schema.setPrecisions(precisions);
            if (opDescriptionJSON.contains("nullValue") && !opDescriptionJSON["nullValue"].is_null()) {
                schema.setNullValue(opDescriptionJSON["nullValue"]);
            }

            std::vector<int> csvSelectFieldToProjectFieldMapping =
                opDescriptionJSON["csvSelectFieldToProjectFieldMapping"];
            std::vector<int> csvSelectFieldToCsvFieldMapping = opDescriptionJSON["csvSelectFieldToCsvFieldMapping"];
            std::vector<int> oneMap;
            oneMap.resize(csvSelectFieldToProjectFieldMapping.size());
            for (size_t i = 0; i < csvSelectFieldToProjectFieldMapping.size(); i++) {
                oneMap[csvSelectFieldToProjectFieldMapping[i]] = csvSelectFieldToCsvFieldMapping[i];
            }
            // use small batch size for testing
            constexpr int batchSize = 1000;
            auto csvInputFormat =
                new omnistream::csv::CsvInputFormat<omnistream::VectorBatch>(schema, batchSize, oneMap);
            constexpr int fileLength = 100000;
            InputSplit* inputSplit = new InputSplit(opDescriptionJSON["filePath"], 0, fileLength);

            auto* func = new InputFormatSourceFunction<omnistream::VectorBatch>(csvInputFormat, inputSplit);

            auto* source = new StreamSource<omnistream::VectorBatch>(func, chainOutput, false);
            source->setup(std::move(task));
            // SP-INTEROP: StreamSource 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
            source->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
            return static_cast<StreamOperator*>(source);
        } else if (format == "values") {
            auto* func = new ValuesSourceFunction(opDescriptionJSON);
            auto* source = new StreamSource<omnistream::VectorBatch>(func, chainOutput, false);
            source->setup(std::move(task));
            source->setDescription(opDescriptionJSON);
            // SP-INTEROP: VALUES 源与 csv/nexmark 源一致，无 managed keyed state，
            // Omni 原生格式即为 Flink 兼容。
            source->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
            return static_cast<StreamOperator*>(source);
        } else if (format == "nexmark") {
            int batchSize = opDescriptionJSON["batchSize"];
            BatchEventDeserializer* eventDeserializer = new BatchEventDeserializer(batchSize);
            auto typeInfo = TypeInfoFactory::createTypeInfo("String");
            // In NexmarkConfiguration, all values have been set to their default value
            NexmarkConfiguration nexmarkConfig(opDescriptionJSON);
            GeneratorConfig config{nexmarkConfig};

            auto* func = new NexmarkSourceFunction<omnistream::VectorBatch>(config, eventDeserializer, typeInfo);
            auto* source = new StreamSource<omnistream::VectorBatch>(func, chainOutput, false);
            source->setup(std::move(task));
            source->setDescription(opDescriptionJSON);
            // SP-INTEROP: StreamSource 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
            source->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
            return static_cast<StreamOperator*>(source);
        } else if (format == "joinSource") {
            // In JoinSource, all values have been set to their default value
            JoinSource* func = new JoinSource(opDescriptionJSON);
            auto* source = new StreamSource<omnistream::VectorBatch>(func, chainOutput, false);
            source->setup(std::move(task));
            source->setDescription(opDescriptionJSON);
            // SP-INTEROP: StreamSource 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
            source->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
            return static_cast<StreamOperator*>(source);
        } else if (format == "kafka") {
            auto description = opConfig.getDescription();
            nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
            // create kafka source
            std::shared_ptr<KafkaSource> source = std::make_shared<KafkaSource>(opDescriptionJSON, true);
            ProcessingTimeService* timeService = task->createProcessingTimeService();
            auto* op = new SourceOperator(chainOutput, opDescriptionJSON, source, timeService);
            op->setup(std::move(task));
            if (opDescriptionJSON.value("upsertMode", false)) {
                op->setFlinkSavepointUnsupported("Upsert Kafka source savepoints are unsupported");
                return op;
            }
            // SP-INTEROP: SourceOperator 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
            op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
            LOG("Operator SourceOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return op;
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateStreamExpandOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto* execExpand = new StreamExpand(opDescriptionJSON, chainOutput);
    execExpand->setup(std::move(task));
    // SP-INTEROP: StreamExpand 为无 managed state 的投影算子，与 StreamCalc 同类，设 OmniIsCompatible。
    execExpand->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    LOG("Operator StreamExpand address  " + std::to_string(reinterpret_cast<long>(execExpand)));
    return static_cast<OneInputStreamOperator*>(execExpand);
}

StreamOperator* StreamOperatorFactory::CreateTimestampInserterOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    LOG("create StreamRecordTimestampInserterOperator");
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    nlohmann::json object;
    object["outputfile"] = "/tmp/flink_output.txt";
    auto* op = new TimeStampInserterSinkOperator(object, chainOutput, opDescriptionJSON);
    // SP-INTEROP: TimeStampInserter 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateProcessOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    const nlohmann::json& opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        auto op = new ProcessOperator<Object*, Object*>(opDescriptionJSON, chainOutput, true);
        op->setup(std::move(task));
        LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else if (operatorType == Type_o::SQL) {
        LookupJoinRunner* runner = new LookupJoinRunner(opDescriptionJSON, chainOutput);
        auto op = new ProcessOperator<omnistream::VectorBatch*, omnistream::VectorBatch*>(
            runner, opDescriptionJSON, chainOutput, false);
        op->setup(std::move(task));
        LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator*>(op);
    } else {
        THROW_LOGIC_EXCEPTION("CreateProcessOp: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateConstraintEnforcerOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto* op = new ConstraintEnforcer(chainOutput);
    LOG("Operator ConstraintEnforcer address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateWindowJoinOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::string joinType =
        opDescriptionJSON.contains("joinType") ? opDescriptionJSON["joinType"].get<std::string>() : "InnerJoin";
    WindowJoinOperator<std::shared_ptr<RowData>>* op = nullptr;
    if (joinType == "LeftOuterJoin") {
        op = new LeftOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
    } else if (joinType == "RightOuterJoin") {
        op = new RightOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
    } else if (joinType == "FullOuterJoin") {
        op = new FullOuterJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
    } else {
        op = new InnerJoinOperator<std::shared_ptr<RowData>>(opDescriptionJSON, chainOutput, nullptr, nullptr);
    }
    op->setup(std::move(task));
    // SP-INTEROP: 启用 WindowJoin 兼容适配器
    const FlinkSavepointAdaptorType adaptorType =
        omnistream::StreamingJoinSavepointUtil::getWindowJoinAdaptorType(opDescriptionJSON);
    if (adaptorType == FlinkSavepointAdaptorType::None) {
        op->setFlinkSavepointUnsupported(
            omnistream::StreamingJoinSavepointUtil::buildWindowJoinUnsupportedReason(opDescriptionJSON));
    } else {
        op->setDescription(opDescriptionJSON);
        op->setFlinkSavepointAdaptor(adaptorType);
    }
    LOG("Operator WindowJoinOperator address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<TwoInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateWindowTableFunctionOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto* op = new WindowTableFunctionOperator(opDescriptionJSON, chainOutput);
    // WindowTableFunction is stateless. Processing time (if any) is served by this service.
    auto* processingTimeService = task->createProcessingTimeService();
    op->setProcessingTimeService(processingTimeService);
    op->setup(std::move(task));
    op->setDescription(opDescriptionJSON);
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    LOG("Operator WindowTableFunctionOperator address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateMapOp(
    omnistream::OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        auto* mapOp = new datastream::StreamMap<Object, Object*>(chainOutput, opDescriptionJSON, true);
        return static_cast<OneInputStreamOperator*>(mapOp);
    } else if (operatorType == Type_o::SQL) {
        NOT_IMPL_EXCEPTION;
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateFilterOp(
    omnistream::OperatorPOD& opConfig,
    WatermarkGaugeExposingOutput* chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        auto* filterOp = new datastream::StreamFilter<Object, Object*>(chainOutput, opDescriptionJSON, true);
        return static_cast<OneInputStreamOperator*>(filterOp);
    } else if (operatorType == Type_o::SQL) {
        // operatorType == "sql"
        NOT_IMPL_EXCEPTION;
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateFlatMapOp(
    omnistream::OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        auto* op = new datastream::StreamFlatMap<Object, Object*>(chainOutput, opDescriptionJSON, true);
        return static_cast<OneInputStreamOperator*>(op);
    } else if (operatorType == Type_o::SQL) {
        // operatorType == "sql"
        NOT_IMPL_EXCEPTION;
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateKeyedCoProcessOp(
    omnistream::OperatorPOD& opConfig,
    WatermarkGaugeExposingOutput* chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        auto op = new datastream::KeyedCoProcessOperator<Object, Object*, Object*, Object*>(
            chainOutput, opDescriptionJSON, true);
        op->setup(std::move(task));
        auto twoInputOperator = static_cast<TwoInputStreamOperator*>(op);
        twoInputOperator->SetOperatorID(opConfig.getOperatorId());
        return twoInputOperator;
    } else if (operatorType == Type_o::SQL) {
        // operatorType == "sql"
        NOT_IMPL_EXCEPTION;
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateStreamingFileWriterOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);

    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(opDescriptionJSON);
    int bucketCheckInterval = 1000;

    auto* op = new StreamingFileWriter<omnistream::VectorBatch*>(bucketCheckInterval, bucketsBuilder);
    op->setOutput(chainOutput);
    op->setup();
    // SP-INTEROP: StreamingFileWriter 无 managed keyed state，Omni 原生格式即为 Flink 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);

    LOG("Operator StreamingFileWriter address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreatePartitionCommitterOp(
    OperatorPOD& opConfig, WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto* op = new PartitionCommitter();
    // SP-INTEROP: PartitionCommitter 无 managed state，Omni 原生格式即为 Flink 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);

    LOG("Operator PartitionCommitter address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateReduceOp(
    omnistream::OperatorPOD& opConfig,
    WatermarkGaugeExposingOutput* chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto operatorType = opConfig.getOperatorType(); // 0(NULL), 1(SQL), 2(STREAM)
    if (operatorType == Type_o::STREAM) {
        // 从 opConfig.getInputs()[0] 推导 Tuple/Pojo serializer，作为 reduce 的 ValueState
        // serializer。否则会落到 ObjectSerializer，CP 写入触发 NOT_IMPL_EXCEPTION。
        // 任何解析失败都不应阻塞 operator 构造，回退到 nullptr → operator 内退化为旧路径。
        TypeSerializer* valueSerializer = nullptr;
        try {
            const auto& inputs = opConfig.getInputs();
            if (!inputs.empty()) {
                const auto& inputType = inputs[0];
                TypeInformation* typeInfo = nullptr;
                if (inputType.kind == "basic") {
                    typeInfo = TypeInfoFactory::createTypeInfo(inputType.type.c_str());
                } else if (inputType.kind == "Tuple") {
                    auto fieldType = nlohmann::json::parse(inputType.type);
                    typeInfo = TypeInfoFactory::createTupleTypeInfo(fieldType);
                } else {
                    INFO_RELEASE(
                        "Error:CreateReduceOp: unsupported input kind '" << inputType.kind
                                                                         << "', falling back to ObjectSerializer");
                }
                if (typeInfo != nullptr) {
                    valueSerializer = typeInfo->getTypeSerializer();
                    delete typeInfo;
                    INFO_RELEASE(
                        "CreateReduceOp: built valueSerializer="
                        << (valueSerializer ? valueSerializer->getName() : "null"));
                }
            }
        } catch (const std::exception& e) {
            INFO_RELEASE(
                "Error:CreateReduceOp: exception while building valueSerializer: "
                << e.what() << ", falling back to ObjectSerializer");
            valueSerializer = nullptr;
        } catch (...) {
            INFO_RELEASE("Error:CreateReduceOp: unknown exception while building valueSerializer, falling back");
            valueSerializer = nullptr;
        }
        auto* op =
            new datastream::StreamGroupedReduceOperator<Object>(chainOutput, opDescriptionJSON, true, valueSerializer);
        op->setup(std::move(task));
        // 关键：必须把 operator ID 设到 op 上，否则 CP 时 op->GetOperatorID() 返回默认 OperatorID，
        // JM 把 state 存在那个错误 key 下，restore 用真实 operator ID 查就找不到 → 状态全丢。
        // StreamGroupedReduceOperator 同时继承 AbstractUdfStreamOperator 与 OneInputStreamOperator，
        // 两条路径都看得到 StreamOperator::SetOperatorID（diamond），直接调会 ambiguous，先 cast 一下。
        auto* oneInputOp = static_cast<OneInputStreamOperator*>(op);
        oneInputOp->SetOperatorID(opConfig.getOperatorId());
        return oneInputOp;
    } else if (operatorType == Type_o::SQL) {
        // operatorType == "sql"
        NOT_IMPL_EXCEPTION;
    } else {
        THROW_LOGIC_EXCEPTION("createOperator: invalid operatorType");
    }
}

StreamOperator* StreamOperatorFactory::CreateSinkWriterOp(
    omnistream::OperatorPOD& opConfig,
    WatermarkGaugeExposingOutput* chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task,
    ProcessingTimeService* processingTimeService)
{
    DeliveryGuarantee deliveryGuarantee;
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::string deliveryGuaranteeStr = opDescriptionJSON["deliveryGuarantee"];
    LOG("deliveryGuaranteeStr:" + deliveryGuaranteeStr);
    if (deliveryGuaranteeStr == "EXACTLY_ONCE") {
        deliveryGuarantee = DeliveryGuarantee::EXACTLY_ONCE;
    } else if (deliveryGuaranteeStr == "AT_LEAST_ONCE") {
        deliveryGuarantee = DeliveryGuarantee::AT_LEAST_ONCE;
    } else {
        deliveryGuarantee = DeliveryGuarantee::NONE;
    }
    auto kafkaProducerConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    nlohmann::json kafkaProducerConfigJson = opDescriptionJSON["kafkaProducerConfig"];
    int64_t maxPushRecords = 100000;
    for (const auto& element : kafkaProducerConfigJson.items()) {
        std::string key = element.key();
        std::string value = element.value();
        std::string errorString;

        // check kv
        const std::map<std::string, std::string>::iterator& iter = ProducerConfig.find(key);
        if (iter != ProducerConfig.end() && iter->second != "") {
            kafkaProducerConfig->set(iter->second, value, errorString);
        }

        // opt kafka producer para from file
        std::unordered_map<std::string, std::string> optConsumerConfig = ConfigLoader::GetKafkaProducerConfig();
        for (const auto& pair : optConsumerConfig) {
            if (pair.first == "max.push.records") {
                maxPushRecords = std::stol(pair.second);
            } else {
                kafkaProducerConfig->set(pair.first, pair.second, errorString);
            }
        }

        LOG("kafkaProducerConfig set key:" << key << " value:" << value);
    }
    std::string transactionalIdPrefix = opDescriptionJSON.value("upsertMode", false)
                                            ? std::string{}
                                            : opDescriptionJSON["transactionalIdPrefix"].get<std::string>();
    LOG("transactionalIdPrefix:" + transactionalIdPrefix);
    std::string topic = opDescriptionJSON["topic"];
    LOG("topic:" + topic);
    auto kafkaSink = new KafkaSink(
        deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, topic, opDescriptionJSON, maxPushRecords);

    auto* op = new SinkWriterOperator(kafkaSink, opDescriptionJSON);
    op->setProcessingTimeService(processingTimeService);
    if (opDescriptionJSON.value("upsertMode", false)) {
        op->setFlinkSavepointUnsupported("Upsert Kafka sink savepoints are unsupported");
        return static_cast<OneInputStreamOperator*>(op);
    }
    // SP-INTEROP: SinkWriter 使用 operator state（transactional IDs），无需 keyed state 格式转换，Omni 原生格式即为
    // Flink 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);
    // todo 确认模板参数问题
    return static_cast<OneInputStreamOperator*>(op);
}

StreamOperator* StreamOperatorFactory::CreateCommitOp(
    omnistream::OperatorPOD& opConfig,
    WatermarkGaugeExposingOutput* chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task,
    ProcessingTimeService* processingTimeService)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    bool isBatch = opDescriptionJSON["batch"];
    std::string hashPath = opDescriptionJSON["hash_path"];

    auto* op = new CommitterOperator(processingTimeService, isBatch, true);
    op->setup(std::move(task));
    // SP-INTEROP: Committer 使用 operator state（committable list），无需 keyed state 格式转换，Omni 原生格式即为Flink
    // 兼容。
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible);

    return static_cast<OneInputStreamOperator*>(op);
}
} // namespace omnistream
