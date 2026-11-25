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

#include "OperatorChain.h"
#include <semaphore.h>
#include <atomic>
#include <streaming/api/operators/StreamOperatorFactory.h>
#include "ChainingOutput.h"
#include "streaming/api/operators/CountingOutput.h"
#include "DataStreamChainingOutput.h"
#include <typeinfo/TypeInfoFactory.h>
#include "core/typeutils/LongSerializer.h"
#include "WatermarkGaugeExposingOutput.h"
#include "streaming/api/operators/sink/SinkWriterOperator.h"
#include "streaming/api/operators/sink/CommitterOperator.h"
#include "state/bridge/OmniTaskBridge.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/TwoInputStreamOperator.h"
#include "basictypes/Object.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/RowType.h"
#include "omni/OmniStreamTask.h"
#include "runtime/io/network/api/writer/RecordWriterDelegate.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include <algorithm>
#include <cctype>

namespace {
void AssignConfiguredOperatorId(StreamOperator* op, const omnistream::OperatorPOD& opDesc)
{
    if (op == nullptr) {
        return;
    }
    const std::string operatorId = opDesc.getOperatorId();
    if (!operatorId.empty()) {
        op->SetOperatorID(operatorId);
        if (auto* oneInput = dynamic_cast<OneInputStreamOperator*>(op)) {
            oneInput->SetOperatorID(operatorId);
        }
        if (auto* twoInput = dynamic_cast<TwoInputStreamOperator*>(op)) {
            twoInput->SetOperatorID(operatorId);
        }
        if (auto* rowDataOp = dynamic_cast<AbstractStreamOperator<RowData*>*>(op)) {
            rowDataOp->SetOperatorID(operatorId);
        }
        if (auto* rdRowDataOp = dynamic_cast<AbstractStreamOperator<std::shared_ptr<RowData>>*>(op)) {
            rdRowDataOp->SetOperatorID(operatorId);
        }
        if (auto* objectOp = dynamic_cast<AbstractStreamOperator<Object*>*>(op)) {
            objectOp->SetOperatorID(operatorId);
        }
        if (auto* longOp = dynamic_cast<AbstractStreamOperator<long>*>(op)) {
            longOp->SetOperatorID(operatorId);
        }
        if (auto* voidOp = dynamic_cast<AbstractStreamOperator<void*>*>(op)) {
            voidOp->SetOperatorID(operatorId);
        }
        if (auto* binaryRowDataOp = dynamic_cast<AbstractStreamOperator<BinaryRowData*>*>(op)) {
            binaryRowDataOp->SetOperatorID(operatorId);
        }

        INFO_RELEASE(
            "savepoint: OperatorChainV2 assign operatorId=" << operatorId << " name=" << opDesc.getName()
                                                            << " id=" << opDesc.getId());
    }
}

std::string NormalizeOperatorId(std::string id)
{
    std::transform(
        id.begin(), id.end(), id.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return id;
}

const omnistream::StreamConfigPOD* FindStreamConfigByOperatorId(
    const omnistream::TaskInformationPOD& taskConfiguration,
    const std::vector<omnistream::StreamConfigPOD>& chainedConfig,
    const std::string& operatorId)
{
    const std::string normalizedOperatorId = NormalizeOperatorId(operatorId);
    const auto& headConfig = taskConfiguration.getStreamConfigPOD();
    if (NormalizeOperatorId(headConfig.getOperatorDescription().getOperatorId()) == normalizedOperatorId) {
        return &headConfig;
    }
    for (const auto& streamConfig : chainedConfig) {
        if (NormalizeOperatorId(streamConfig.getOperatorDescription().getOperatorId()) == normalizedOperatorId) {
            return &streamConfig;
        }
    }
    return nullptr;
}

WatermarkGaugeExposingOutput* WrapWithBytesCountingOutput(
    WatermarkGaugeExposingOutput* output,
    const std::shared_ptr<omnistream::OmniStreamTask>& streamTask,
    omnistream::OperatorPOD& opDesc)
{
    if (output == nullptr || streamTask == nullptr || streamTask->env() == nullptr) {
        return output;
    }
    auto metricGroup = streamTask->env()->taskMetricGroup();
    if (metricGroup == nullptr) {
        return output;
    }
    return new BytesCountingOutput(output, metricGroup, opDesc);
}

WatermarkGaugeExposingOutput* WrapWithRecordsCountingOutput(
    WatermarkGaugeExposingOutput* output,
    const std::shared_ptr<omnistream::OmniStreamTask>& streamTask,
    omnistream::OperatorPOD& opDesc)
{
    if (output == nullptr || streamTask == nullptr || streamTask->env() == nullptr) {
        return output;
    }
    auto metricGroup = streamTask->env()->taskMetricGroup();
    if (metricGroup == nullptr) {
        return output;
    }
    return new CountingOutput(output, metricGroup, opDesc);
}

} // namespace

namespace omnistream {

WatermarkGaugeExposingOutput* OperatorChainV2::wrapOperatorIntoOutput(
    StreamOperator* op, omnistream::OperatorPOD& opConfig)
{
    if (!op) {
        INFO_RELEASE("operator is null");
        throw std::runtime_error("operator is null");
    }
    if (!op->canBeStreamOperator()) {
        auto* chainingOutput =
            new ChainingOutput(dynamic_cast<OneInputStreamOperator*>(op), op->GetMectrics(), opConfig);
        return chainingOutput;
    } else {
        return new datastream::DataStreamChainingOutput(dynamic_cast<OneInputStreamOperator*>(op));
    }
}

WatermarkGaugeExposingOutput* OperatorChainV2::createOperatorChain(
    const std::shared_ptr<OmniStreamTask>& streamTask,
    StreamConfigPOD* operatorConfig,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs,
    std::vector<StreamOperatorWrapper*>& allOperatorWrappers)
{
    auto chainedOperatorOutput =
        createOutputCollector(streamTask, operatorConfig[0], chainedConfigs, recordWriterOutputs, allOperatorWrappers);

    auto opDesc = operatorConfig[0].getOperatorDescription();
    auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainedOperatorOutput, streamTask);
    AssignConfiguredOperatorId(chainedOperator, opDesc);

    auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
    allOperatorWrappers.emplace_back(operatorWrapper);
    // wrapOperatorIntoOutput hands back a raw new; the chain owns it for its own lifetime.
    auto* chainedOutput = wrapOperatorIntoOutput(chainedOperator, opDesc);
    ownedOutputs_.push_back(chainedOutput);
    return chainedOutput;
}

WatermarkGaugeExposingOutput* OperatorChainV2::createDataStreamOperatorChain(
    StreamConfigPOD& operatorConfig,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs,
    std::vector<StreamOperatorWrapper*>& allOperatorWrappers)
{
    auto chainedOperatorOutput =
        createDataStreamOutputCollector(operatorConfig, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

    auto opDesc = operatorConfig.getOperatorDescription();
    auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainedOperatorOutput, nullptr);
    AssignConfiguredOperatorId(chainedOperator, opDesc);

    registerHandler(opDesc, chainedOperator);

    auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
    allOperatorWrappers.emplace_back(operatorWrapper);

    return wrapOperatorIntoOutput(chainedOperator, opDesc);
}

WatermarkGaugeExposingOutput* OperatorChainV2::createOutputCollector(
    const std::shared_ptr<OmniStreamTask>& streamTask,
    StreamConfigPOD& operatorConfig,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs,
    std::vector<StreamOperatorWrapper*>& allOperatorWrappers)
{
    std::vector<WatermarkGaugeExposingOutput*> allOutputs;

    for (auto output : operatorConfig.getOpNonChainedOutputs()) {
        int key = static_cast<int>(std::hash<omnistream::NonChainedOutputPOD>{}(output));
        auto recordWriterOutput = recordWriterOutputs[key];
        allOutputs.emplace_back(recordWriterOutput);
    }

    for (auto outputEdge : operatorConfig.getOpChainedOutputs()) {
        int outputId = outputEdge.getTargetId();
        auto chainedOpConfig = chainedConfigs[outputId];
        auto* pPod = new StreamConfigPOD[2]{chainedOpConfig, operatorConfig};
        auto output = createOperatorChain(streamTask, pPod, chainedConfigs, recordWriterOutputs, allOperatorWrappers);
        // createOperatorChain only copies out of pPod, so the array is dead once it returns.
        delete[] pPod;
        allOutputs.emplace_back(output);
    }

    WatermarkGaugeExposingOutput* result;
    if (allOutputs.size() == 1) {
        result = allOutputs[0];
    } else {
        if (streamTask->getTaskType() == 1) {
            result = new VectorBatchCopyingBroadcastingOutputCollector(allOutputs);
        } else if (streamTask->getTaskType() == 2) {
            result = new datastream::CopyingBroadcastingOutputCollector(allOutputs);
        } else {
            THROW_LOGIC_EXCEPTION("not support task type: " + std::to_string(streamTask->getTaskType()));
        }
    }
    auto opDesc = operatorConfig.getOperatorDescription();
    result = WrapWithBytesCountingOutput(result, streamTask, opDesc);
    return WrapWithRecordsCountingOutput(result, streamTask, opDesc);
}

WatermarkGaugeExposingOutput* OperatorChainV2::createDataStreamOutputCollector(
    StreamConfigPOD& operatorConfig,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs,
    std::vector<StreamOperatorWrapper*>& allOperatorWrappers)
{
    std::vector<WatermarkGaugeExposingOutput*> allOutputs;

    for (auto output : operatorConfig.getOpNonChainedOutputs()) {
        auto recordWriterOutput = recordWriterOutputs[output.getSourceNodeId()];
        allOutputs.emplace_back(recordWriterOutput);
    }

    for (auto outputEdge : operatorConfig.getOpChainedOutputs()) {
        int outputId = outputEdge.getTargetId();
        auto chainedOpConfig = chainedConfigs[outputId];

        auto output =
            createDataStreamOperatorChain(chainedOpConfig, chainedConfigs, recordWriterOutputs, allOperatorWrappers);
        allOutputs.emplace_back(output);
    }

    if (allOutputs.size() == 1) {
        return allOutputs[0];
    } else {
        return new datastream::CopyingBroadcastingOutputCollector(allOutputs);
    }
}

void OperatorChainV2::linkOperatorWrappers(std::vector<StreamOperatorWrapper*>& allOperatorWrappers)
{
    StreamOperatorWrapper* previous = nullptr;
    for (auto current : allOperatorWrappers) {
        if (previous != nullptr) {
            previous->setPrevious(current);
        }
        current->setNext(previous);
        previous = current;
    }
}

OperatorChainV2::OperatorChainV2(
    std::weak_ptr<OmniStreamTask> containingTask, std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate)
{
    if (auto streamTask = containingTask.lock()) {
        TaskInformationPOD taskConfiguration = streamTask->env()->taskConfiguration();

        // since SQL operator chain support, use the operator chain descriptor directly

        auto configuration = taskConfiguration.getStreamConfigPOD();
        auto outputsInOrder = configuration.getOutEdgesInOrder();
        auto chainedConfigs = taskConfiguration.getChainedConfigMap();

        std::unordered_map<int, RecordWriterOutputV2*> recordWriterOutputs;
        streamOutputs.resize(outputsInOrder.size());
        createChainOutputs(outputsInOrder, chainedConfigs, recordWriterDelegate, recordWriterOutputs);

        std::vector<StreamOperatorWrapper*> allOperatorWrappers;
        this->mainOperatorOutput =
            createOutputCollector(streamTask, configuration, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

        auto opDesc = configuration.getOperatorDescription();
        auto chainedOperator =
            StreamOperatorFactory::createOperatorAndCollector(opDesc, mainOperatorOutput, streamTask);
        AssignConfiguredOperatorId(chainedOperator, opDesc);
        registerHandler(opDesc, chainedOperator);
        auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
        this->mainOperatorWrapper = operatorWrapper;
        allOperatorWrappers.emplace_back(operatorWrapper);
        this->tailOperatorWrapper = allOperatorWrappers[0];
        linkOperatorWrappers(allOperatorWrappers);

        operatorDependenciesDeal();
    } else {
        THROW_LOGIC_EXCEPTION("Object has been deleted!\n");
    }
}

// TypeInformation *OperatorChainV2::getChainOutputType(OperatorChainPOD &opChainDesc)
TypeInformation* OperatorChainV2::getChainOutputType(OperatorPOD operatorPod)
{
    LOG("Beginning of  getChainOutputType ");
    auto& lastOperator = operatorPod; // operators are in reverse order

    LOG("after  getOperatorDesc" << lastOperator.toString());

    auto lastOperatorOutput = lastOperator.getOutput();

    LOG("after  getOperatorConfig:" << lastOperatorOutput.toString());

    TypeInformation* typeInfo;

    if (lastOperatorOutput.kind == "basic") {
        std::string outputTypeName = lastOperatorOutput.type;
        typeInfo = TypeInfoFactory::createTypeInfo(outputTypeName.c_str());
    } else if (lastOperatorOutput.kind == "Row") {
        LOG("row type description is:" << lastOperatorOutput.type);
        nlohmann::json outputRowType = nlohmann::json::parse(lastOperatorOutput.type);
        typeInfo = TypeInfoFactory::createInternalTypeInfo(outputRowType);
    } else if (lastOperatorOutput.kind == "Tuple") {
        nlohmann::json outputType = nlohmann::json::parse(lastOperatorOutput.type);
        typeInfo = TypeInfoFactory::createTupleTypeInfo(outputType);
    } else if (lastOperatorOutput.kind == "CommittableMessage") {
        typeInfo = TypeInfoFactory::createCommittableMessageInfo();
    } else {
        auto description = nlohmann::json::parse(operatorPod.getDescription());
        typeInfo = TypeInfoFactory::createDataStreamTypeInfo(description["outputTypes"]);
    }

    LOG("after  createTypeInfo");

    return typeInfo;
}

TypeInformation* OperatorChainV2::getDataStreamStateKeyType(OperatorPOD operatorPod)
{
    auto description = nlohmann::json::parse(operatorPod.getDescription());
    return TypeInfoFactory::createDataStreamTypeInfo(description["stateKeyTypes"]);
}

TypeInformation* OperatorChainV2::getDataStreamChainOutputType(OperatorPOD operatorPod)
{
    auto description = nlohmann::json::parse(operatorPod.getDescription());
    return TypeInfoFactory::createDataStreamTypeInfo(description["outputTypes"]);
}

void OperatorChainV2::finishOperators(StreamTaskActionExecutor* actionExecutor)
{
    auto opWrap = mainOperatorWrapper;
    while (opWrap != nullptr) {
        auto op = opWrap->getStreamOperator();
        op->finish();
        opWrap = opWrap->getNext();
    }
}

RecordWriterOutputV2* OperatorChainV2::createStreamOutput(
    RecordWriterV2* recordWriter, TypeInformation& typeInformation, const NonChainedOutputPOD& streamOutput)
{
    LOG("typeInformation.name()" << typeInformation.name());
    TypeSerializer* serializer = typeInformation.getTypeSerializer();
    serializer->setSelfBufferReusable(true);
    LOG("After creation of serializer " << serializer->getName());
    return new RecordWriterOutputV2(recordWriter, serializer, streamOutput.getSupportsUnalignedCheckpoints());
}

datastream::RecordWriterOutput* OperatorChainV2::createDataStreamStreamOutput(
    datastream::RecordWriter* recordWriter, TypeInformation& typeInformation)
{
    TypeSerializer* serializer = typeInformation.createTypeSerializer();
    LOG("After creation of serializer " << serializer->getName());
    return new datastream::RecordWriterOutput(serializer, recordWriter);
}

void OperatorChainV2::createChainOutputs(
    std::vector<StreamEdgePOD>& outputsInOrder,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate,
    std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs)
{
    LOG("Before call  createChainOutputs ");
    std::unordered_map<int, int> indexForSource;
    for (size_t i = 0; i < outputsInOrder.size(); i++) {
        auto output = outputsInOrder[i];
        int sourceId = output.getSourceId();
        auto streamConfig = chainedConfigs[sourceId];
        const auto& nonChainedOutputs = streamConfig.getOpNonChainedOutputs();
        TypeInformation* chainOutputType = getChainOutputType(streamConfig.getOperatorDescription());

        int index = indexForSource[sourceId]++;
        LOG("TypeInformation is " << chainOutputType->name());
        auto recordWriterOutput =
            createStreamOutput(recordWriterDelegate->getRecordWriter(i), *chainOutputType, nonChainedOutputs[index]);
        streamOutputs[i] = recordWriterOutput;
        // getChainOutputType hands back a raw new that nobody owned. TypeInformation derives from
        // Object, so release the reference rather than deleting: putRefCount frees it only if no
        // one else took a reference. Only the serializer inside it is still needed --
        // createStreamOutput passed that to the RecordWriterOutput, and neither ~TypeInformation
        // nor ~InternalTypeInfo touches it.
        chainOutputType->putRefCount();
        chainOutputType = nullptr;
        const auto& nonChainedOutput = nonChainedOutputs[index];
        int key = static_cast<int>(std::hash<omnistream::NonChainedOutputPOD>{}(nonChainedOutput));
        recordWriterOutputs[key] = recordWriterOutput;
    }
    LOG("After call  createChainOutputs ");
}

void OperatorChainV2::createDataStreamChainOutputs(
    std::vector<StreamEdgePOD>& outputsInOrder,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::shared_ptr<datastream::RecordWriterDelegate> recordWriterDelegate,
    std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs)
{
    LOG("Before call  createDataStreamChainOutputs ");
    for (size_t i = 0; i < outputsInOrder.size(); i++) {
        auto output = outputsInOrder[i];
        auto streamConfig = chainedConfigs[output.getSourceId()];
        TypeInformation* chainOutputType = getDataStreamChainOutputType(streamConfig.getOperatorDescription());

        auto recordWriterOutput =
            createDataStreamStreamOutput(recordWriterDelegate->getRecordWriter(i), *chainOutputType);
        delete chainOutputType;
        recordWriterOutputs[output.getSourceId()] = recordWriterOutput;
    }
    LOG("After call  createDataStreamChainOutputs ");
}

StreamOperator* OperatorChainV2::createMainOperatorAndCollector(
    OperatorChainPOD& opChainDesc, RecordWriterOutputV2* chainOutput)
{
    // operatorA--OperatorB---OperatorC

    // Generating the last operator first and wrap it with the RecordWriterOutput.
    LOG(">> chaining with " << opChainDesc.toString() << " operators...");
    auto operators = opChainDesc.getOperators();

    // `operators` is vector of `OperatorPOD` in reverse order
    OperatorPOD opDesc = operators[0]; // last operator
    if (opDesc.getId() == "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer") {
        operators.erase(operators.begin());
        opDesc = operators[0];
    }

    StreamOperator* op = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainOutput, nullptr);
    AssignConfiguredOperatorId(op, opDesc);
    tailOperatorWrapper = new StreamOperatorWrapper(op, false);

    // Connect the operators in reverse order
    auto nextOpWrapper = tailOperatorWrapper;
    ChainingOutput* chainingOutput;
    for (size_t i = 1; i < operators.size(); i++) {
        LOG(">> generating chainingOutput" + i);
        chainingOutput = new ChainingOutput(static_cast<OneInputStreamOperator*>(op));
        // this operator need chainingOutput of its next operator, which has already been created
        opDesc = operators[i];
        LOG(">> generating operator " + opDesc.getId() + " and wrap the chainingOutput ");
        if (opDesc.getId() == "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer") continue;
        op = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainingOutput, nullptr);
        AssignConfiguredOperatorId(op, opDesc);
        auto OpWrapper = new StreamOperatorWrapper(op, false);
        OpWrapper->setNext(nextOpWrapper);
        nextOpWrapper->setPrevious(OpWrapper);
        nextOpWrapper = OpWrapper;
    }
    mainOperatorWrapper = nextOpWrapper;
    mainOperatorWrapper->setAsHead();
    // set the last StreamOperatorWrapper as the mainStreamOperatorWrapper.

    // set the mainOperatorOutput
    if (operators.size() > 1) {
        mainOperatorOutput = chainingOutput;
    } else {
        mainOperatorOutput = chainOutput;
    }

    operatorDependenciesDeal();

    return op;
}

static TypeSerializer* tryCreateRowDataSerializerFromJson(const nlohmann::json& description)
{
    // Helper: try to build keyTypeNames from (keyFieldName, typeFieldName)
    auto tryBuild = [&](const char* keyField, const char* typeField) -> TypeSerializer* {
        if (!description.contains(keyField)) return nullptr;
        if (!description.contains(typeField) || description[typeField].empty()) return nullptr;

        std::vector<int32_t> keyIndices;
        if (!description[keyField].empty()) {
            keyIndices = description[keyField].get<std::vector<int32_t>>();
        }

        auto typeNames = description[typeField].get<std::vector<std::string>>();
        std::vector<std::string> keyTypeNames;
        keyTypeNames.reserve(keyIndices.size());
        for (int32_t idx : keyIndices) {
            if (idx >= 0 && static_cast<size_t>(idx) < typeNames.size()) {
                keyTypeNames.push_back(typeNames[idx]);
            }
        }
        // Create RowDataSerializer even with empty keyTypeNames (arity=0),
        // matching Flink's RowDataSerializer(LogicalType[0]) behavior for global window aggregate.
        auto* rowType = new omnistream::RowType(true, keyTypeNames);
        auto* serializer = new RowDataSerializer(rowType);
        delete rowType;
        return serializer;
    };

    TypeSerializer* ser = nullptr;

    // grouping + inputTypes (Agg/Dedup/Rank)
    ser = tryBuild("grouping", "inputTypes");
    if (ser) return ser;

    // partitionKey + inputTypes (Rank/Dedup)
    ser = tryBuild("partitionKey", "inputTypes");
    if (ser) return ser;

    // leftJoinKey + leftInputTypes (StreamingJoin / WindowJoin)
    ser = tryBuild("leftJoinKey", "leftInputTypes");
    if (ser) return ser;

    // rightJoinKey + rightInputTypes (StreamingJoin / WindowJoin — fallback)
    ser = tryBuild("rightJoinKey", "rightInputTypes");
    if (ser) return ser;

    INFO_RELEASE("OperatorChainV2: tryCreateRowDataSerializerFromJson, no matching keys found");
    return nullptr;
}

void OperatorChainV2::initializeStateAndOpenOperators(
    StreamTaskStateInitializerImpl* initializer, const TaskInformationPOD& taskConfiguration_)
{
    // call operators' initializeState() and open() in a reverse order.
    LOG("OperatorChainV2::initializeStateAndOpenOperators start");
    std::vector<StreamConfigPOD> chainedConfig = taskConfiguration_.getChainedConfig();
    int index = 0;
    auto allOperators = getAllOperators(false); // positive sequence
    while (allOperators.hasNext()) {
        auto operatorWrapper = allOperators.next();
        auto streamOperator = operatorWrapper->getStreamOperator();
        const std::string runtimeOperatorId = streamOperator->GetOperatorID().toString();
        const StreamConfigPOD* streamConfigPOD =
            FindStreamConfigByOperatorId(taskConfiguration_, chainedConfig, runtimeOperatorId);
        if (streamConfigPOD == nullptr && index < static_cast<int>(chainedConfig.size())) {
            streamConfigPOD = &chainedConfig[index];
        }
        if (streamConfigPOD == nullptr) {
            LOG("Error: no StreamConfig for operatorId=" << runtimeOperatorId << ", index=" << index
                                                         << ", chainedConfigSize=" << chainedConfig.size());
            THROW_LOGIC_EXCEPTION("no StreamConfig for operatorId=" << runtimeOperatorId);
        }
        index++;
        const OperatorPOD& operatorPod = streamConfigPOD->getOperatorDescription();
        if (auto* named = dynamic_cast<NamedOperator*>(streamOperator)) {
            named->SetOpName(operatorPod.getName());
        }
        const nlohmann::json& description = nlohmann::json::parse(operatorPod.getDescription());
        int operatorType = operatorPod.getOperatorType();
        switch (operatorType) {
            case Type_o::INVALID: // NULL
                THROW_LOGIC_EXCEPTION("invalid operatorType");
                break;
            case Type_o::SQL: // SQL
            {
                int keyArity = 0;
                TypeSerializer* keySer = tryCreateRowDataSerializerFromJson(description);
                if (keySer != nullptr) {
                    // Determine keyArity from whichever key field matched
                    if (description.contains("grouping") && !description["grouping"].empty())
                        keyArity = static_cast<int>(description["grouping"].get<std::vector<int32_t>>().size());
                    else if (description.contains("leftJoinKey") && !description["leftJoinKey"].empty())
                        keyArity = static_cast<int>(description["leftJoinKey"].get<std::vector<int32_t>>().size());
                    else if (description.contains("partitionKey") && !description["partitionKey"].empty())
                        keyArity = static_cast<int>(description["partitionKey"].get<std::vector<int32_t>>().size());
                } else {
                    INFO_RELEASE(
                        "OperatorChainV2::initializeStateAndOpenOperators fallback BinaryRowDataSerializer keyArity="
                        << keyArity);
                    keySer = new BinaryRowDataSerializer(keyArity);
                }
                streamOperator->initializeState(initializer, keySer);
            } break;
            case Type_o::STREAM: // STREAM
                if (!description.contains("stateKeyTypes") || description["stateKeyTypes"].empty()) {
                    // streamOperator is a stateless operator
                    streamOperator->initializeState(initializer, nullptr);
                } else {
                    TypeInformation* typeInfo = getDataStreamStateKeyType(operatorPod);
                    TypeSerializer* typeSerializer = typeInfo->createTypeSerializer();
                    streamOperator->initializeState(initializer, typeSerializer);
                    delete typeInfo;
                }
                break;
            default: THROW_LOGIC_EXCEPTION("jobType does not support in initializeStateAndOpenOperators");
        }
        streamOperator->open();
    }

    LOG("OperatorChainV2::initializeStateAndOpenOperators end");
}

void OperatorChainV2::DispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
{
    LOG("OperatorChainV2::dispatchOperatorEvent start >> operatorId={" + operatorIdString + "} >> event={" +
        eventString + "}");
    auto it = handlers.find(operatorIdString);
    if (it != handlers.end()) {
        it->second->handleOperatorEvent(eventString);
    } else {
        LOG("OperatorChainV2::dispatchOperatorEvent cannot find corresponding event handler");
    }
    LOG("OperatorChainV2::dispatchOperatorEvent end");
}

void OperatorChainV2::PrepareSnapshotPreBarrier(long checkpointId)
{
    // go forward through the operator chain and tell each operator
    // to prepare the checkpoint
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink first check if op is closed. We don't have isClosed() now.
        auto op = iter.next()->getStreamOperator();
        op->PrepareSnapshotPreBarrier(checkpointId);
    }
}

void OperatorChainV2::NotifyCheckpointComplete(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            op->notifyCheckpointComplete(checkpointId);
        } catch (...) {
            throw std::runtime_error("notifyCheckpointComplete failed");
        }
    }
}

void OperatorChainV2::NotifyCheckpointAborted(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            op->notifyCheckpointAborted(checkpointId);
        } catch (...) {
            throw std::runtime_error("notifyCheckpointAborted failed");
        }
    }
}

void OperatorChainV2::NotifyCheckpointSubsumed(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            op->notifyCheckpointSubsumed(checkpointId);
        } catch (...) {
            throw std::runtime_error("notifyCheckpointSubsumed failed");
        }
    }
}

void OperatorChainV2::SnapshotState(
    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
    CheckpointMetaData& checkpointMetaData,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<Supplier<bool>> isRunning,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
    std::shared_ptr<CheckpointStreamFactory> storage,
    const std::shared_ptr<OmniTaskBridge>& bridge)
{
    try {
        auto iter = getAllOperators(true);
        while (iter.hasNext()) {
            auto op = iter.next()->getStreamOperator();
            auto operatorId = op->GetOperatorID();
            if (operatorSnapshotsInProgress->find(operatorId) != operatorSnapshotsInProgress->end()) {
                INFO_RELEASE(
                    "Error: OperatorChainV2::SnapshotState duplicate operatorId="
                    << operatorId.toString() << ", opType=" << typeid(*op).name()
                    << ". Duplicate operator IDs would overwrite checkpoint state.");
                THROW_LOGIC_EXCEPTION(
                    "Duplicate operatorId in OperatorChainV2::SnapshotState: " << operatorId.toString());
            }
            (*operatorSnapshotsInProgress)[operatorId] = BuildOperatorSnapshotFutures(
                checkpointMetaData, checkpointOptions, op, isRunning, channelStateWriteResult, storage, bridge);
        }
        SendAcknowledgeCheckpointEvent(checkpointMetaData.GetCheckpointId());
    } catch (...) {
        throw std::runtime_error("snapshotState failed");
    }
}

OperatorSnapshotFutures* OperatorChainV2::BuildOperatorSnapshotFutures(
    CheckpointMetaData checkpointMetaData,
    CheckpointOptions* checkpointOptions,
    StreamOperator* op,
    std::shared_ptr<Supplier<bool>> isRunning,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
    std::shared_ptr<CheckpointStreamFactory> storage,
    const std::shared_ptr<OmniTaskBridge>& bridge)
{
    OperatorSnapshotFutures* snapshotInProgress =
        CheckpointStreamOperator(op, checkpointMetaData, checkpointOptions, storage, isRunning, bridge);
    if (channelStateWriteResult->IsNeedsChannelState()) {
        SnapshotChannelStates(op, channelStateWriteResult, snapshotInProgress);
    }
    return snapshotInProgress;
}

std::string OperatorChainV2::checkCompatibleSavepointSupport()
{
    std::string unsupportedReasons;
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        auto* op = iter.next()->getStreamOperator();
        FlinkSavepointAdaptorInfo adaptorInfo;
        if (auto aop = dynamic_cast<AbstractStreamOperator<RowData*>*>(op)) {
            adaptorInfo = aop->getSavepointAdaptorInfo();
        } else if (auto rd_aop = dynamic_cast<AbstractStreamOperator<std::shared_ptr<RowData>>*>(op)) {
            adaptorInfo = rd_aop->getSavepointAdaptorInfo();
        } else if (auto sop = dynamic_cast<AbstractStreamOperator<Object*>*>(op)) {
            adaptorInfo = sop->getSavepointAdaptorInfo();
        } else if (auto lop = dynamic_cast<AbstractStreamOperator<long>*>(op)) {
            adaptorInfo = lop->getSavepointAdaptorInfo();
        } else if (auto vop = dynamic_cast<AbstractStreamOperator<void*>*>(op)) {
            adaptorInfo = vop->getSavepointAdaptorInfo();
        } else if (auto bop = dynamic_cast<AbstractStreamOperator<BinaryRowData*>*>(op)) {
            adaptorInfo = bop->getSavepointAdaptorInfo();
        } else {
            // 非 AbstractStreamOperator 的算子无 keyed state，跳过
            continue;
        }
        if (adaptorInfo.type == FlinkSavepointAdaptorType::None) {
            if (!unsupportedReasons.empty()) {
                unsupportedReasons += "; ";
            }
            unsupportedReasons += adaptorInfo.reason;
        }
    }
    return unsupportedReasons;
}

OperatorSnapshotFutures* OperatorChainV2::CheckpointStreamOperator(
    StreamOperator* op,
    CheckpointMetaData checkpointMetaData,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<CheckpointStreamFactory> storageLocation,
    std::shared_ptr<Supplier<bool>> isRunning,
    const std::shared_ptr<OmniTaskBridge>& bridge)
{
    try {
        INFO_RELEASE("savepoint: OperatorChainV2::CheckpointStreamOperator op type=" << typeid(*op).name());
        /* 部分算子存在菱形继承问题，需要转成 AbstractStreamOperator ，例如：SinkWriterOperator [OneInputStreamOperator,
         * AbstractStreamOperator] -> StreamOperator */
        auto aop = dynamic_cast<AbstractStreamOperator<RowData*>*>(op);
        if (aop) {
            return aop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        auto rd_aop = dynamic_cast<AbstractStreamOperator<std::shared_ptr<RowData>>*>(op);
        if (rd_aop) {
            return rd_aop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        auto sop = dynamic_cast<AbstractStreamOperator<Object*>*>(op);
        if (sop) {
            return sop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        auto lop = dynamic_cast<AbstractStreamOperator<long>*>(op);
        if (lop) {
            return lop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        auto vop = dynamic_cast<AbstractStreamOperator<void*>*>(op);
        if (vop) {
            return vop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        auto bop = dynamic_cast<AbstractStreamOperator<BinaryRowData*>*>(op);
        if (bop) {
            return bop->SnapshotState(
                checkpointMetaData.GetCheckpointId(),
                checkpointMetaData.GetTimestamp(),
                checkpointOptions,
                storageLocation,
                bridge);
        }
        INFO_RELEASE("savepoint: OperatorChainV2::CheckpointStreamOperator StreamOperator::SnapshotState");
        /* 规避：增加其他处理，不转换直接调用 StreamOperator 的 SnapshotState 什么也没做 */
        return op->SnapshotState(
            checkpointMetaData.GetCheckpointId(),
            checkpointMetaData.GetTimestamp(),
            checkpointOptions,
            storageLocation,
            bridge);
    } catch (...) {
        throw std::runtime_error("checkpointStreamOperator failed");
    }
}
void OperatorChainV2::SendAcknowledgeCheckpointEvent(long checkpointId)
{
    if (operatorEventDispatcher == nullptr) {
        return;
    }

    auto registeredOperators = operatorEventDispatcher->GetRegisteredOperators();
    std::for_each(registeredOperators.begin(), registeredOperators.end(), [this, checkpointId](const auto& x) {
        operatorEventDispatcher->GetOperatorEventGateway(x)->SendEventToCoordinator(
            std::make_unique<AcknowledgeCheckpointEvent>(checkpointId));
    });
}

void OperatorChainV2::SnapshotChannelStates(
    StreamOperator* op,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
    OperatorSnapshotFutures* snapshotInProgress)
{
    StreamOperator* mainOpe = (mainOperatorWrapper == nullptr) ? nullptr : mainOperatorWrapper->getStreamOperator();
    if (mainOpe == op) {
        snapshotInProgress->OperatorSemInit();
        channelStateWriteResult->GetInputChannelStateHandles()->ThenApply(
            [snapshotInProgress](
                const std::shared_ptr<std::vector<std::shared_ptr<InputChannelStateHandle>>>& handles_ptr) {
                if (!handles_ptr) {
                    snapshotInProgress->OperatorSemPost();
                    return;
                }
                std::shared_ptr<StateObjectCollection<InputChannelStateHandle>> collection =
                    std::make_shared<StateObjectCollection<InputChannelStateHandle>>(*handles_ptr);
                auto snapshotResult = SnapshotResult<StateObjectCollection<InputChannelStateHandle>>::Of(collection);
                using PackagedTaskType = std::packaged_task<
                    std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>;
                PackagedTaskType task([snapshotResult]() { return snapshotResult; });
                auto task_ptr = std::make_shared<PackagedTaskType>(std::move(task));
                snapshotInProgress->setInputChannelStateFuture(task_ptr);
                snapshotInProgress->OperatorSemPost();
            });
    }
    StreamOperator* tailOpe = (tailOperatorWrapper == nullptr) ? nullptr : tailOperatorWrapper->getStreamOperator();
    if (op == tailOpe) {
        snapshotInProgress->OperatorSemInit();
        channelStateWriteResult->GetResultSubpartitionStateHandles()->ThenApply(
            [snapshotInProgress](
                const std::shared_ptr<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>& handles_ptr) {
                if (!handles_ptr) {
                    snapshotInProgress->OperatorSemPost();
                    return;
                }
                std::shared_ptr<StateObjectCollection<ResultSubpartitionStateHandle>> collection =
                    std::make_shared<StateObjectCollection<ResultSubpartitionStateHandle>>(*handles_ptr);
                auto snapshotResult =
                    SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>::Of(collection);
                using PackagedTaskType = std::packaged_task<
                    std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>;
                PackagedTaskType task([snapshotResult]() { return snapshotResult; });
                auto task_ptr = std::make_shared<PackagedTaskType>(std::move(task));
                snapshotInProgress->setResultSubpartitionStateFuture(task_ptr);
                snapshotInProgress->OperatorSemPost();
            });
    }
}

void OperatorChainV2::operatorDependenciesDeal()
{
    if (this->mainOperatorWrapper == nullptr) {
        GErrorLog("OperatorDependenciesDeal mainOperatorWrapper is nullptr");
        return;
    }
    std::unordered_map<std::string_view, StreamOperator*> opMap;
    StreamOperatorWrapper* current = this->mainOperatorWrapper;
    while (current != nullptr) {
        StreamOperator* op = current->getStreamOperator();
        if (dynamic_cast<CommitterOperator<>*>(op) != nullptr) {
            opMap.emplace(OPERATOR_NAME_COMMIT_OPERATOR, op);
        }
        if (dynamic_cast<SinkWriterOperator*>(op) != nullptr) {
            opMap.emplace(OPERATOR_NAME_SINK_WRITER, op);
        }
        current = current->getNext();
    }
    if (opMap.empty()) {
        INFO_RELEASE("OperatorDependenciesDeal opMap empty");
        return;
    }
    if (opMap.find(OPERATOR_NAME_COMMIT_OPERATOR) != opMap.end()) {
        CommitterOperator<>* committerOperator =
            dynamic_cast<CommitterOperator<>*>(opMap[OPERATOR_NAME_COMMIT_OPERATOR]);
        if (committerOperator == nullptr) {
            GErrorLog("OperatorDependenciesDeal CommiterOperator not CommitterOperator");
            return;
        }
        if (committerOperator->hasCommitter()) {
            // An independently scheduled Kafka committer initializes from its
            // descriptor and does not need a writer in the same operator chain.
            return;
        }
        if (opMap.find(OPERATOR_NAME_SINK_WRITER) == opMap.end()) {
            GErrorLog("OperatorDependenciesDeal CommiterOperator dependy SinkWriterOperator");
            return;
        }
        SinkWriterOperator* sinkWriterOperator = dynamic_cast<SinkWriterOperator*>(opMap[OPERATOR_NAME_SINK_WRITER]);
        if (sinkWriterOperator == nullptr) {
            GErrorLog("OperatorDependenciesDeal CommiterOperator not CommitterOperator");
            return;
        }
        committerOperator->initFromKafkaSink(sinkWriterOperator->getKafkaSink());
    } else {
        INFO_RELEASE("OperatorDependenciesDeal not need deal");
    }
}
} // namespace omnistream
