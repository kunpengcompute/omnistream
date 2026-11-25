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

#include "StreamTask.h"
#include "runtime/io/network/api/writer/SingleRecordWriter.h"
#include "runtime/io/network/api/writer/NonRecordWriter.h"
#include "runtime/io/network/api/writer/MultipleRecordWriters.h"
#include "../typeinfo/TypeInfoFactory.h"
#include "streaming/runtime/partitioner/RebalancePartitioner.h"
#include "streaming/runtime/partitioner/RescalePartitioner.h"
#include "streaming/runtime/partitioner/GlobalPartitioner.h"
#include "streaming/runtime/partitioner/KeyGroupStreamPartitioner.h"
#include "streaming/runtime/io/StreamTaskNetworkInputFactory.h"
#include "streaming/runtime/partitioner/ForwardPartitioner.h"
#include <filesystem>
#include "api/common/TaskInfoImpl.h"
#include "runtime/io/network/api/writer/V2/MultipleRecordWritersV2.h"
#include "runtime/io/network/api/writer/V2/NonRecordWriterV2.h"
#include "runtime/io/network/api/writer/V2/RecordWriterBuilderV2.h"
#include "runtime/io/network/api/writer/V2/SingleRecordWriterV2.h"
#include "streaming/runtime/partitioner/V2/ForwardPartitionerV2.h"
#include "streaming/runtime/partitioner/V2/GlobalPartitionerV2.h"
#include "streaming/runtime/partitioner/V2/KeyGroupStreamPartitionerV2.h"
#include "streaming/runtime/partitioner/V2/RebalancePartitionerV2.h"
#include "streaming/runtime/partitioner/V2/RescalePartitionerV2.h"
#include "streaming/runtime/partitioner/V2/SimplePartitioner.h"

namespace omnistream::datastream {
StreamTask::StreamTask(const nlohmann::json& ntdd, void* outputBufferStatus, std::shared_ptr<RuntimeEnvironmentV2> env)
    : taskPartitionerConfig(extractTaskPartitionerConfig(ntdd)),
      outputBufferStatus_(reinterpret_cast<OutputBufferStatus*>(outputBufferStatus)),
      env_(env)
{
    createTask(ntdd);
}

StreamTask::~StreamTask()
{
}

void StreamTask::cleanUp()
{
    delete operatorChain_;
}

// setup
int StreamTask::createTask(const json& ntdd)
{
    LOG(">>>Begin of createTask");

    // json ntdd to object tdd
    extractOperatorChainConfig(operatorChainConfig_, ntdd);

    LOG(">>>Setup RecordWriter");
    setupRecordWriter();

    LOG(">>>Setup OperatorChain");
    setupOperatorChain(operatorChainConfig_);

    LOG(">>> End of create StreamTask ");
    return 0;
};

int StreamTask::setupRecordWriter()
{
    LOG("setupRecordWriter outputBuffer_ address " + std::to_string(outputBufferStatus_->outputBuffer_) + " capacity " +
        std::to_string(static_cast<int>(outputBufferStatus_->capacity_)));
    TaskInformationPOD taskConfig = env_->taskConfiguration();

    recordWriter_ = createRecordWriterDelegate(taskConfig, env_);
    return 0;
};

int StreamTask::setupOperatorChain(std::vector<OperatorConfig>& operatorChainConfig)
{
    operatorChain_ = new omnistream::OperatorChainV2(env_, recordWriter_);
    LOG("After after OperatorChain ");
    auto& taskConfiguration_ = env_->taskConfiguration();

    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env_.get());
    operatorChain_->initializeStateAndOpenOperators(initializer, taskConfiguration_);
    mainOperator_ = operatorChain_->getMainOperator();

    LOG("After createMainOperatorAndCollector " + std::to_string(reinterpret_cast<long>(mainOperator_)));

    return 0;
}

int StreamTask::extractOperatorChainConfig(std::vector<OperatorConfig>& chainConfig, const json& ntdd)
{
    // Check types
    if (ntdd["operators"].is_array()) {
        for (const auto& element : ntdd["operators"]) {
            json inputType = element["description"]["inputTypes"]; // can be kind:basic or kind:row
            const json& outputType = element["description"]["outputTypes"];

            std::string name = element["name"];
            std::string uniqueName = element["id"];
            json description = element["description"];
            OperatorConfig opConfig(uniqueName, name, inputType, outputType, description);
            chainConfig.push_back(std::move(opConfig));
        }
    } else {
        THROW_LOGIC_EXCEPTION("NTDD JSON object is not well formed : " + to_string(ntdd));
    }

    return 0;
}

TaskPartitionerConfig StreamTask::extractTaskPartitionerConfig(const json& ntdd)
{
    if (ntdd.contains("partition")) {
        json options = nullptr;
        json partitionInfo = ntdd["partition"];
        std::string partitionName = partitionInfo["partitionName"];
        int numberOfChannel = partitionInfo["channelNumber"];
        if (partitionName == "hash") {
            options = partitionInfo["hashFields"];
        }

        return TaskPartitionerConfig(partitionName, numberOfChannel, options);
    } else {
        // todo
        LOG("the partition info is missing.......");
        // Temporary dummy partition info for sink vertex
        return TaskPartitionerConfig("forward", 1, nullptr);
    }
}

std::unique_ptr<StreamTaskNetworkInput> StreamTask::createDataInput(std::vector<long>& chanelInfos)
{
    LOG(">>>>> Begin");
    auto operatorPod = env_->taskConfiguration().getStreamConfigPOD().getOperatorDescription();
    auto description = nlohmann::json::parse(operatorPod.getDescription());
    auto inputTypes = description["inputTypes"];
    uint32_t channelGateIndex = 0;
    // channelIndex = static_cast<uint32_t>(chanelInfos[0] & 0xFFFFFFFF);//currently, channelIndex is not used
    channelGateIndex = static_cast<uint32_t>((chanelInfos[0] >> 32) & 0xFFFFFFFF);
    if (!inputTypes.empty() && inputTypes[channelGateIndex].is_object()) {
        auto inputTypeInfo = TypeInfoFactory::createDataStreamTypeInfo(inputTypes[channelGateIndex]);
        return StreamTaskNetworkInputFactory::create(inputTypeInfo->createTypeSerializer(), chanelInfos);
    } else {
        THROW_LOGIC_EXCEPTION("Invalid channel input index.");
    }
}

std::unique_ptr<StreamTaskNetworkOutput> StreamTask::createDataOutput(int operatorMethodIndicator)
{
    // workaround, as for now streamflatmap operator is input
    LOG(">>> main operator address" + std::to_string(reinterpret_cast<long>(mainOperator_)));
    auto streamTaskNetworkOutput = std::make_unique<StreamTaskNetworkOutput>(mainOperator_, operatorMethodIndicator);
    return streamTaskNetworkOutput;
}

void StreamTask::resetOutputBufferAndRecordWriter()
{
    if (recordWriter_->getRecordWriter(0) == nullptr) {
        return;
    }
    int32_t numberOfResult = 0;
    int32_t size = 0;
    LOG("after  input_->emitNextProcessElement numberOfResult :" + std::to_string(numberOfResult) + " size " +
        std::to_string(size));

    LOG("after  input_->emitNextProcessElement  outputBufferStatus_ address:" +
        std::to_string(reinterpret_cast<long>(outputBufferStatus_)));
    LOG("after  input_->emitNextProcessElement  outputBuffer addess:" +
        std::to_string(outputBufferStatus_->outputBuffer_));
    outputBufferStatus_->numberElement = numberOfResult;
    outputBufferStatus_->outputSize = size;

    // record writer clear for next call
    clearOutputBuffer();
}

void StreamTask::clearOutputBuffer()
{
}

OutputBufferStatus* StreamTask::getOutputBufferStatus()
{
    return outputBufferStatus_;
}

StreamPartitioner<IOReadableWritable>* StreamTask::createPartitioner()
{
    string partitionName = taskPartitionerConfig.getPartitionName();
    if (partitionName == "forward") {
        return new omnistream::datastream::ForwardPartitioner<IOReadableWritable>();
    } else if (partitionName == "rescale") {
        return new omnistream::datastream::RescalePartitioner<IOReadableWritable>();
    } else if (partitionName == "rebalance") {
        return new RebalancePartitioner<IOReadableWritable>();
    } else if (partitionName == "global") {
        return new GlobalPartitioner<IOReadableWritable>();
    } else if (partitionName == "hash") {
        return nullptr;
    } else {
        THROW_LOGIC_EXCEPTION("Partitioner not implemented");
    }
}

void StreamTask::addStreamOneInputProcessor(StreamOneInputProcessor* processor)
{
    this->streamOneInputProcessors.push_back(processor);
}

// Get a StreamOneInputProcessor from the vector by index
StreamOneInputProcessor* StreamTask::getStreamOneInputProcessor(size_t index)
{
    if (index >= streamOneInputProcessors.size()) {
        throw std::out_of_range("Index out of range");
    }
    return streamOneInputProcessors[index];
}

StreamOneInputProcessor* StreamTask::createOmniInputProcessor(const json& inputChannelInfo, int operatorMethodIndicator)
{
    if (inputChannelInfo.contains("input_channels")) {
        const json& channelInfos = inputChannelInfo["input_channels"];
        // Check if the array is present and valid

        if (channelInfos.is_array()) {
            // Get the size of the array
            int array_size = channelInfos.size();

            // Create a C++ normal array (vector)
            std::vector<long> channel_array(array_size);

            // Copy elements from JSON array to C++ array
            for (int i = 0; i < array_size; ++i) {
                channel_array[i] = channelInfos[i];
            }

            LOG(">>> createDataInput with numbe of channel" << array_size);
            return new StreamOneInputProcessor(
                createDataOutput(operatorMethodIndicator), createDataInput(channel_array));
        } else {
            THROW_LOGIC_EXCEPTION("No Input channel info provided in TDD.");
        }
    } else {
        THROW_LOGIC_EXCEPTION("No Input channel info provided in TDD.");
    }
}

std::shared_ptr<RecordWriterDelegateV2> StreamTask::createRecordWriterDelegate(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    std::vector<RecordWriterV2*> recordWriters = createRecordWriters(taskConfig, environment);
    LOG("TaskInformation " << taskConfig.toString() << "and recorderWriter size " << recordWriters.size());
    if (recordWriters.size() == 1) {
        return std::make_shared<SingleRecordWriterV2>(recordWriters[0]);
    } else if (recordWriters.empty()) {
        return std::make_shared<NonRecordWriterV2>();
    } else {
        return std::make_shared<MultipleRecordWritersV2>(recordWriters);
    }
}

std::vector<RecordWriterV2*> StreamTask::createRecordWriters(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    auto recordWriters = std::vector<RecordWriterV2*>();
    auto outEdgesInOrder = taskConfig.getStreamConfigPOD().getOutEdgesInOrder();
    for (size_t i = 0; i < outEdgesInOrder.size(); i++) {
        auto outEdge = outEdgesInOrder[i];
        recordWriters.push_back(createRecordWriter(outEdge, i, environment, "taskName_", outEdge.getBufferTimeout()));
    }
    return recordWriters;
}

RecordWriterV2* StreamTask::createRecordWriter(
    StreamEdgePOD& edge,
    int outputIndex,
    std::shared_ptr<RuntimeEnvironmentV2> environment,
    std::string taskName,
    long bufferTimeout)
{
    // output partitioner

    auto partitioner = edge.getPartitioner();

    auto outputPartitioner = createPartitionerFromDesc(edge);
    // workaround for now
    if (outputPartitioner == nullptr) {
        THROW_RUNTIME_ERROR("outputPartitioner is null"); // the below can not work
    }

    auto bufferPartitionWriter = env_->writers()[outputIndex];
    // we initialize the partitioner here with the number of key groups (aka max. parallelism)
    if (typeid(*outputPartitioner) == typeid(KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>)) {
        int numKeyGroups = bufferPartitionWriter->getNumTargetKeyGroups();
        if (0 < numKeyGroups) {
            reinterpret_cast<KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>*>(outputPartitioner)
                ->configure(numKeyGroups);
        }
    }

    auto writer = RecordWriterBuilderV2()
                      .withChannelSelector(outputPartitioner)
                      .withWriter(bufferPartitionWriter)
                      .withTimeout(bufferTimeout)
                      .withTaskName(taskName)
                      .withJobType(1)
                      .build();

    writer->postConstruct();
    return writer;
}

// record write deleage
StreamPartitioner<IOReadableWritable>* StreamTask::createPartitionerFromDesc(const StreamEdgePOD& edge)
{
    const auto& partitioner = edge.getPartitioner();

    if (partitioner.getPartitionerName() == StreamPartitionerPOD::FORWARD) {
        return new ForwardPartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::REBALANCE) {
        return new RebalancePartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::RESCALE) {
        return new RescalePartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::HASH) {
        int targetId = edge.getTargetId();
        int sourceId = edge.getSourceId();
        auto taskInfo = env_->taskConfiguration();
        std::unordered_map<int, StreamConfigPOD> configMap = taskInfo.getChainedConfigMap();
        auto description = configMap[sourceId].getOperatorDescription().getDescription();
        nlohmann::json config = nlohmann::json::parse(description);
        return new KeyGroupStreamPartitioner<IOReadableWritable, Object>(config, targetId, 128);
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::NONE) {
        throw std::invalid_argument("Invalid partitioner!");
    } else {
        throw std::invalid_argument("Invalid partitioner!");
    }
}

template <typename K>
KeySelector<K>* StreamTask::buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields)
{
    std::vector<int> keyCols;
    std::vector<int> keyTypes;
    for (auto& keyField : keyFields) {
        keyCols.emplace_back(keyField.getFieldIndex());
        if (keyField.getFieldTypeName().compare("BIGINT") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_LONG);
        }
        if (keyField.getFieldTypeName().compare("VARCHAR") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_VARCHAR);
        }
        if (keyField.getFieldTypeName().compare("INTEGER") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_INT);
        }
    }
    return new KeySelector<K>(keyTypes, keyCols);
}
} // namespace omnistream::datastream
