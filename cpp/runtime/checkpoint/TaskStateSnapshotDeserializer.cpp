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

#include "TaskStateSnapshotDeserializer.h"

std::shared_ptr<TaskStateSnapshot> TaskStateSnapshotDeserializer::Deserialize(const std::string& jsonString)
{
    const json j = json::parse(jsonString);
    auto snapshot = std::make_shared<TaskStateSnapshot>();
    // Accept both field name variants: Java serializer uses "isTaskDeployedAsFinished",
    // while some code paths use "taskDeployedAsFinished"
    if (j.contains("isTaskDeployedAsFinished")) {
        snapshot->SetIsTaskDeployedAsFinished(j.at("isTaskDeployedAsFinished").get<bool>());
    } else if (j.contains("taskDeployedAsFinished")) {
        snapshot->SetIsTaskDeployedAsFinished(j.at("taskDeployedAsFinished").get<bool>());
    }
    if (j.contains("isTaskFinished")) {
        snapshot->SetIsTaskFinished(j.at("isTaskFinished").get<bool>());
    } else if (j.contains("taskFinished")) {
        snapshot->SetIsTaskFinished(j.at("taskFinished").get<bool>());
    }
    const json& subtask_states_json = j.at("subtaskStatesByOperatorID");
    for (auto& el : subtask_states_json.items()) {
        if (el.key() == "@class") {
            continue;
        }
        OperatorID id = HexStringToOperatorId<OperatorID>(el.key());
        snapshot->PutSubtaskStateByOperatorID(id, ParseOperatorSubtaskState(el.value()));
    }

    return snapshot;
}

std::shared_ptr<KeyedStateHandle> TaskStateSnapshotDeserializer::ParseKeyedStateHandle(const json& j)
{
    LOG("savepoint: ParseKeyedStateHandle: " << j.dump());
    if (!j.contains("@class")) {
        throw std::runtime_error("State handle JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    // Dispatch to the correct parser based on the class name
    if (className.find("IncrementalRemoteKeyedStateHandle") != std::string::npos) {
        return ParseRemoteStateHandle(j);
    }
    if (className.find("DirectoryKeyedStateHandle") != std::string::npos) {
        return ParseDirectoryKeyedStateHandle(j);
    }
    if (className.find("IncrementalLocalKeyedStateHandle") != std::string::npos) {
        return ParseLocalStateHandle(j);
    }
    if (className.find("KeyGroupsSavepointStateHandle") != std::string::npos) {
        return ParseKeyGroupsSavepointStateHandle(j);
    }
    // KeyGroupsStateHandle is used by Flink's native HashMapStateBackend for checkpoint
    if (className.find("KeyGroupsStateHandle") != std::string::npos) {
        return ParseKeyGroupsStateHandle(j);
    }

    throw std::runtime_error("Unsupported or unknown KeyedStateHandle type: " + className);
}

std::shared_ptr<OperatorStateHandle> TaskStateSnapshotDeserializer::ParseOperatorStateHandle(const json& j)
{
    LOG("ParseOperatorStateHandle: " << j.dump());
    if (!j.contains("@class")) {
        throw std::runtime_error("State handle JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    if (className.find("OperatorStreamStateHandle") == std::string::npos) {
        LOG("ERROR: State handle JSON is error, className: " << className);
        throw std::runtime_error("State handle JSON is error, className: " + className);
    };
    return std::make_shared<OperatorStreamStateHandle>(j);
}

std::shared_ptr<ResultSubpartitionStateHandle> TaskStateSnapshotDeserializer::ParseResultStateHandle(const json& j)
{
    LOG("ParseResultStateHandle: " << j.dump());
    if (!j.contains("@class")) {
        LOG("ERROR: State handle JSON is missing the '@class' field.");
        throw std::runtime_error("State handle JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    if (className.find("ResultSubpartitionStateHandle") == std::string::npos) {
        LOG("ERROR: State handle JSON is error, className: " << className);
        throw std::runtime_error("State handle JSON is error, className: " + className);
    }
    int subTaskIndex = j["subtaskIndex"].get<int>();
    ResultSubpartitionInfoPOD info;
    std::shared_ptr<StreamStateHandle> delegate;
    const std::string className2 = j["delegate"].at("@class").get<std::string>();
    if (className2.find("ByteStreamStateHandle") != std::string::npos) {
        std::string handleName = j["delegate"]["handleName"].get<std::string>();
        std::string encodedData = j["delegate"]["data"].get<std::string>();
        std::vector<uint8_t> decodedData = Base64_decode(encodedData);
        delegate = std::make_shared<ByteStreamStateHandle>(handleName, decodedData);
    } else if (className2.find("RelativeFileStateHandle") != std::string::npos) {
        delegate = std::make_shared<RelativeFileStateHandle>(j["delegate"]);
    } else {
        LOG("ERROR: Not support for StreamStateHandle type: " << className2);
        throw std::runtime_error("Not support for StreamStateHandle type: " + className2);
    }
    const std::string className3 = j["info"].at("@class").get<std::string>();
    if (className3.find("ResultSubpartitionInfo") != std::string::npos) {
        int partitionIdx = j["info"]["partitionIdx"].get<int>();
        int subPartitionIdx = j["info"]["subPartitionIdx"].get<int>();
        info = ResultSubpartitionInfoPOD(partitionIdx, subPartitionIdx);
    }
    std::vector<int64_t> offsets = j["offsets"][1].get<std::vector<int64_t>>();
    int64_t size = j["size"].get<int64_t>();
    AbstractChannelStateHandle<ResultSubpartitionInfoPOD>::StateContentMetaInfo metaInfo(offsets, size);
    return std::make_shared<ResultSubpartitionStateHandle>(subTaskIndex, info, delegate, metaInfo);
}

std::shared_ptr<InputChannelStateHandle> TaskStateSnapshotDeserializer::ParseInputStateHandle(const json& j)
{
    LOG("ParseInputStateHandle: " << j.dump());
    if (!j.contains("@class")) {
        LOG("ERROR: State handle JSON is missing the '@class' field.");
        throw std::runtime_error("State handle JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    if (className.find("InputChannelStateHandle") == std::string::npos) {
        LOG("ERROR: State handle JSON is error, className: " << className);
        throw std::runtime_error("State handle JSON is error, className: " + className);
    }
    int subTaskIndex = j["subtaskIndex"].get<int>();
    omnistream::InputChannelInfo info;
    std::shared_ptr<StreamStateHandle> delegate;
    const std::string className2 = j["delegate"].at("@class").get<std::string>();
    if (className2.find("ByteStreamStateHandle") != std::string::npos) {
        std::string handleName = j["delegate"]["handleName"].get<std::string>();
        std::string encodedData = j["delegate"]["data"].get<std::string>();
        std::vector<uint8_t> decodedData = Base64_decode(encodedData);
        delegate = std::make_shared<ByteStreamStateHandle>(handleName, decodedData);
    } else if (className2.find("RelativeFileStateHandle") != std::string::npos) {
        delegate = std::make_shared<RelativeFileStateHandle>(j["delegate"]);
    } else {
        LOG("ERROR: Not support for StreamStateHandle type: " << className2);
        throw std::runtime_error("Not support for StreamStateHandle type: " + className2);
    }
    const std::string className3 = j["info"].at("@class").get<std::string>();
    if (className3.find("InputChannelInfo") != std::string::npos) {
        int partitionIdx = j["info"]["gateIdx"].get<int>();
        int subPartitionIdx = j["info"]["inputChannelIdx"].get<int>();
        info = InputChannelInfo(partitionIdx, subPartitionIdx);
    }
    std::vector<int64_t> offsets = j["offsets"][1].get<std::vector<int64_t>>();
    int64_t size = j["size"].get<int64_t>();
    AbstractChannelStateHandle<InputChannelInfo>::StateContentMetaInfo metaInfo(offsets, size);
    return std::make_shared<InputChannelStateHandle>(subTaskIndex, info, delegate, metaInfo);
}

std::shared_ptr<InflightDataRescalingDescriptor> TaskStateSnapshotDeserializer::ParseInflightDataRescalingDescriptor(
    const json& j)
{
    LOG("ParseInflightDataRescalingDescriptor: " << j.dump());
    if (!j.contains("@class")) {
        LOG("ERROR: State handle JSON is missing the '@class' field.");
        throw std::runtime_error("Rescaling descriptor JSON is missing the '@class' field.");
    }
    const std::string className = j.at("@class").get<std::string>();
    if (className.find("InflightDataRescalingDescriptor") == std::string::npos) {
        LOG("ERROR: State handle JSON is error, className: " << className);
        throw std::runtime_error("State handle JSON is error, className: " + className);
    }
    std::vector<InflightDataGateOrPartitionRescalingDescriptor> gateOrPartitionDescriptors;
    auto mappingsJson = j.at("gateOrPartitionDescriptors");
    if (mappingsJson.size() >= 2 && mappingsJson[0].is_string() && mappingsJson[1].is_array()) {
        mappingsJson = mappingsJson[1]; // 取实际描述符数组
    }
    if (mappingsJson.empty()) {
        LOG("GateOrPartitionDescriptors is empty");
        return nullptr;
    }
    for (auto& mappingJson : mappingsJson) {
        const std::string className2 = mappingJson.at("@class").get<std::string>();
        if (className2.find("InflightDataGateOrPartitionRescalingDescriptor") != std::string::npos) {
            std::vector<int> oldSubtaskIndexes = mappingJson["oldSubtaskIndexes"].get<std::vector<int>>();
            std::set<int> ambiguousSubtaskIndexes;
            auto& idxJson = mappingJson["ambiguousSubtaskIndexes"];
            if (idxJson.is_array() && idxJson.size() == 2 && idxJson[0].is_string()) {
                ambiguousSubtaskIndexes = idxJson[1].get<std::set<int>>();
            } else {
                ambiguousSubtaskIndexes = idxJson.get<std::set<int>>();
            }
            InflightDataGateOrPartitionRescalingDescriptor::MappingType mappingType;
            std::shared_ptr<RescaleMappings> rescaleMappings;
            std::string mappingTypeStr = mappingJson["mappingType"].get<std::string>();
            if (mappingTypeStr.find("RESCALING") != std::string::npos) {
                mappingType = InflightDataGateOrPartitionRescalingDescriptor::MappingType::RESCALING;
            } else if (mappingTypeStr.find("IDENTITY") != std::string::npos) {
                mappingType = InflightDataGateOrPartitionRescalingDescriptor::MappingType::IDENTITY;
            } else {
                LOG("ERROR: Not support for mapping type: " << mappingTypeStr);
                throw std::runtime_error("ERROR: Not support for mapping type: " + mappingTypeStr);
            }
            const std::string className3 = mappingJson["rescaledChannelsMappings"].at("@class").get<std::string>();
            if (className3.find("RescaleMappings") != std::string::npos) {
                int numberOfSources = mappingJson["rescaledChannelsMappings"]["numberOfSources"].get<int>();
                int numberOfTargets = mappingJson["rescaledChannelsMappings"]["numberOfTargets"].get<int>();
                const auto& rescaledMappings =
                    mappingJson["rescaledChannelsMappings"]["mappings"].get<std::vector<std::vector<int>>>();
                if (rescaledMappings.empty()) {
                    rescaleMappings = std::make_shared<IdentityRescaleMappings>(numberOfSources, numberOfTargets);
                } else {
                    rescaleMappings =
                        std::make_shared<RescaleMappings>(numberOfSources, rescaledMappings, numberOfTargets);
                }
            } else {
                LOG("ERROR: Not support for rescaledChannelsMappings type: " << className3);
                throw std::runtime_error("ERROR: Not support for rescaledChannelsMappings type: " + className3);
            }
            InflightDataGateOrPartitionRescalingDescriptor descriptor(
                oldSubtaskIndexes, rescaleMappings, ambiguousSubtaskIndexes, mappingType);
            gateOrPartitionDescriptors.emplace_back(descriptor);
        } else {
            LOG("ERROR: Not support for RescalingDescriptor type: " << className2);
            throw std::runtime_error("ERROR: Not support for RescalingDescriptor type: " + className2);
        }
    }
    return std::make_shared<InflightDataRescalingDescriptor>(gateOrPartitionDescriptors);
}

std::shared_ptr<OperatorSubtaskState> TaskStateSnapshotDeserializer::ParseOperatorSubtaskState(const json& j)
{
    auto managedKeyedStateCol =
        j.contains("managedKeyedState")
            ? ParseStateObjectCollection<KeyedStateHandle>(j.at("managedKeyedState"), &ParseKeyedStateHandle)
            : std::make_shared<StateObjectCollection<KeyedStateHandle>>();

    auto managedOperatorStateCol =
        j.contains("managedOperatorState")
            ? ParseStateObjectCollection<OperatorStateHandle>(j.at("managedOperatorState"), &ParseOperatorStateHandle)
            : std::make_shared<StateObjectCollection<OperatorStateHandle>>();

    auto rawKeyedStateCol = j.contains("rawKeyedState") ? ParseStateObjectCollection<KeyedStateHandle>(
                                                              j.at("rawKeyedState"), &ParseKeyedStateHandle)
                                                        : std::make_shared<StateObjectCollection<KeyedStateHandle>>();

    auto inputChannelStateCol =
        j.contains("inputChannelState")
            ? ParseStateObjectCollection<InputChannelStateHandle>(j.at("inputChannelState"), &ParseInputStateHandle)
            : std::make_shared<StateObjectCollection<InputChannelStateHandle>>();

    auto resultSubpartitionStateCol = j.contains("resultSubpartitionState")
                                          ? ParseStateObjectCollection<ResultSubpartitionStateHandle>(
                                                j.at("resultSubpartitionState"), &ParseResultStateHandle)
                                          : std::make_shared<StateObjectCollection<ResultSubpartitionStateHandle>>();

    std::shared_ptr<InflightDataRescalingDescriptor> inputRescalingDescriptorCol;
    if (j.contains("inputRescalingDescriptor")) {
        inputRescalingDescriptorCol = ParseInflightDataRescalingDescriptor(j.at("inputRescalingDescriptor"));
    }
    if (inputRescalingDescriptorCol == nullptr) {
        inputRescalingDescriptorCol = std::make_shared<NoRescalingDescriptor>();
    }
    LOG("Input rescaling descriptor: " << inputRescalingDescriptorCol->ToString());

    std::shared_ptr<InflightDataRescalingDescriptor> outputRescalingDescriptorCol;
    if (j.contains("outputRescalingDescriptor")) {
        outputRescalingDescriptorCol = ParseInflightDataRescalingDescriptor(j.at("outputRescalingDescriptor"));
    }
    if (outputRescalingDescriptorCol == nullptr) {
        outputRescalingDescriptorCol = std::make_shared<NoRescalingDescriptor>();
    }
    LOG("Output rescaling descriptor: " << outputRescalingDescriptorCol->ToString());
    StateObjectCollection<KeyedStateHandle> managedKeyedStateHandles;
    if (managedKeyedStateCol) {
        // This calls a constructor of StateObjectCollection
        managedKeyedStateHandles = StateObjectCollection<KeyedStateHandle>(managedKeyedStateCol->ToArray());
    }
    StateObjectCollection<OperatorStateHandle> managedOperatorState;
    if (managedOperatorStateCol) {
        // This calls a constructor of StateObjectCollection
        managedOperatorState = StateObjectCollection<OperatorStateHandle>(managedOperatorStateCol->ToArray());
    }
    StateObjectCollection<InputChannelStateHandle> inputChannelStates;
    if (inputChannelStateCol) {
        // This calls a constructor of StateObjectCollection
        inputChannelStates = StateObjectCollection<InputChannelStateHandle>(inputChannelStateCol->ToArray());
    }
    StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionStates;
    if (resultSubpartitionStateCol) {
        // This calls a constructor of StateObjectCollection
        resultSubpartitionStates =
            StateObjectCollection<ResultSubpartitionStateHandle>(resultSubpartitionStateCol->ToArray());
    }

    StateObjectCollection<OperatorStateHandle> rawOperatorState; // Empty lvalue
    StateObjectCollection<KeyedStateHandle> rawKeyedState;
    if (rawKeyedStateCol) {
        rawKeyedState = StateObjectCollection<KeyedStateHandle>(rawKeyedStateCol->ToArray());
    }

    auto subtaskState = std::make_shared<OperatorSubtaskState>(
        managedOperatorState,
        rawOperatorState,
        managedKeyedStateHandles, // The only one with data
        rawKeyedState,
        inputChannelStates,          // The only one with data
        resultSubpartitionStates,    // The only one with data
        inputRescalingDescriptorCol, // The only one with data
        outputRescalingDescriptorCol // The only one with data
    );

    return subtaskState;
}

template <typename T>
std::shared_ptr<StateObjectCollection<T>> TaskStateSnapshotDeserializer::ParseStateObjectCollection(
    const json& j, std::shared_ptr<T> (*parser)(const json&))
{
    const int maxSize = 2;
    auto collection = std::make_shared<StateObjectCollection<T>>();
    const json* stateObjects = nullptr;

    if (j.is_array() && j.size() == maxSize && j.at(0).is_string() && j.at(1).is_array()) {
        // Jackson/Flink style: ["StateObjectCollection", [ ... ]]
        stateObjects = &j.at(1);
    } else if (j.is_object() && j.contains("stateObjects") && j.at("stateObjects").is_array()) {
        // OmniAdaptor style: {"stateObjects": [ ... ]}
        stateObjects = &j.at("stateObjects");
    } else if (j.is_array()) {
        // Plain array, used by some tests/adaptors.
        stateObjects = &j;
    }

    if (stateObjects == nullptr) {
        return collection;
    }
    for (const auto& item_json : *stateObjects) {
        collection->Add(parser(item_json));
    }
    return collection;
}
