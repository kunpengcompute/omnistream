/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <algorithm>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "StreamingJoinSavepointAdaptor.h"
#include "StreamingJoinSavepointUtil.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

namespace omnistream {

StreamingJoinSavepointAdaptor::StreamingJoinSavepointAdaptor(FlinkSavepointAdaptorType adaptorType)
    : adaptorType_(adaptorType)
{
}

void StreamingJoinSavepointAdaptor::prepareSidePlans(const nlohmann::json& operatorDescription)
{
    SidePlan leftPlan;
    leftPlan.stateName = StreamingJoinSavepointUtil::LEFT_STATE_NAME;
    leftPlan.outerJoinState = adaptorType_ == FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor;
    parseInputTypes(leftPlan, operatorDescription, StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD);

    SidePlan rightPlan;
    rightPlan.stateName = StreamingJoinSavepointUtil::RIGHT_STATE_NAME;
    rightPlan.outerJoinState = false;
    parseInputTypes(rightPlan, operatorDescription, StreamingJoinSavepointUtil::RIGHT_INPUT_TYPES_FIELD);

    if (leftPlan.inputTypeNames.empty() || rightPlan.inputTypeNames.empty()) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::prepareSidePlans ->"
            << " leftInputTypeCount=" << leftPlan.inputTypeNames.size() << ", rightInputTypeCount="
            << rightPlan.inputTypeNames.size() << ", adaptorType=" << static_cast<int>(adaptorType_));
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::prepareSidePlans left/right inputTypes must not be empty");
    }

    leftPlan_ = std::move(leftPlan);
    rightPlan_ = std::move(rightPlan);
}

void StreamingJoinSavepointAdaptor::parseInputTypes(
    SidePlan& sidePlan, const nlohmann::json& description, const std::string& fieldName)
{
    sidePlan.inputTypeNames.clear();
    sidePlan.inputTypeIds.clear();
    if (!description.contains(fieldName) || !description[fieldName].is_array()) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::parseInputTypes ->" << " fieldName=" << fieldName
                                                                << ", containsField=" << description.contains(fieldName)
                                                                << ", descriptionSize=" << description.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseInputTypes missing input type array field=" + fieldName);
    }

    sidePlan.inputTypeNames = parseStringArray(description, fieldName);
    if (sidePlan.inputTypeNames.size() != description[fieldName].size() ||
        std::any_of(sidePlan.inputTypeNames.begin(), sidePlan.inputTypeNames.end(), [](const std::string& typeName) {
            return typeName.empty();
        })) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::parseInputTypes ->"
            << " fieldName=" << fieldName << ", fieldSize=" << description[fieldName].size()
            << ", parsedTypeCount=" << sidePlan.inputTypeNames.size());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseInputTypes invalid input type field=" + fieldName);
    }
    sidePlan.inputTypeIds = convertToDataTypes(sidePlan.inputTypeNames);
    if (std::any_of(sidePlan.inputTypeIds.begin(), sidePlan.inputTypeIds.end(), [](const auto typeId) {
            return typeId == omniruntime::type::DataTypeId::OMNI_INVALID;
        })) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::parseInputTypes ->" << " fieldName=" << fieldName
                                                                << ", contains unsupported input type");
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseInputTypes unsupported input type field=" + fieldName);
    }
}

void StreamingJoinSavepointAdaptor::prepareForSave(const nlohmann::json& operatorDescription)
{
    prepareSidePlans(operatorDescription);
}

void StreamingJoinSavepointAdaptor::prepareForRestore(const nlohmann::json& operatorDescription)
{
    prepareSidePlans(operatorDescription);
}

void StreamingJoinSavepointAdaptor::validateForSave(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    const std::set<BackendDataType> keyBackendTypes{BackendDataType::ROW_BK, BackendDataType::SHARED_ROW_BK};
    // Heap/RocksDB 目前仍可能登记未使用的 stateName+"vb" 元数据；WithVB 在这里仅用于
    // 显式消费该可选遗留表，当前 StreamingJoin 转换不会读取它。
    validator.requireKeyedMapStateWithVB(
        StreamingJoinSavepointUtil::LEFT_STATE_NAME,
        BackendDataType::VOID_NAMESPACE_BK,
        keyBackendTypes,
        {leftPlan_.outerJoinState ? BackendDataType::TUPLE_INT32_INT32 : BackendDataType::INT_BK});
    validator.requireKeyedMapStateWithVB(
        StreamingJoinSavepointUtil::RIGHT_STATE_NAME,
        BackendDataType::VOID_NAMESPACE_BK,
        keyBackendTypes,
        {rightPlan_.outerJoinState ? BackendDataType::TUPLE_INT32_INT32 : BackendDataType::INT_BK});
    validator.requireNoMoreStates();
}

void StreamingJoinSavepointAdaptor::validateForRestore(
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    const std::set<BackendDataType> leftValueBackendTypes =
        leftPlan_.outerJoinState
            ? std::set<BackendDataType>{BackendDataType::TUPLE_OBJ_OBJ_BK, BackendDataType::TUPLE_INT32_INT32}
            : std::set<BackendDataType>{BackendDataType::INT_BK};
    validator.requireKeyedMapState(
        StreamingJoinSavepointUtil::LEFT_STATE_NAME,
        BackendDataType::VOID_NAMESPACE_BK,
        {BackendDataType::ROW_BK},
        leftValueBackendTypes);
    validator.requireKeyedMapState(
        StreamingJoinSavepointUtil::RIGHT_STATE_NAME,
        BackendDataType::VOID_NAMESPACE_BK,
        {BackendDataType::ROW_BK},
        {BackendDataType::INT_BK});
    validator.requireNoMoreStates();
}

void StreamingJoinSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& keyGroupOffsets,
    FullSnapshotResources& snapshotResources,
    std::string keySerializer)
{
    VectorBatchSavePlan plan = buildSavePlan(snapshotResources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, keyGroupOffsets, snapshotResources, std::move(keySerializer));
}

void StreamingJoinSavepointAdaptor::restore(
    SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}

StateMetaInfoSnapshot StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo(
    int /*kvStateId*/, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    const SidePlan& sidePlan = sidePlanForState(flinkMetaInfo.getName());
    TypeSerializer* namespaceSerializer = flinkMetaInfo.getNamespaceSerializer();
    if (namespaceSerializer == nullptr) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo -> stateName=" << flinkMetaInfo.getName()
                                                                                 << ", namespaceSerializer=null");
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo missing namespace serializer for state=" +
            flinkMetaInfo.getName());
    }
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        ERROR_RELEASE(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo ->"
            << " stateName=" << flinkMetaInfo.getName()
            << ", namespaceBackendId=" << namespaceSerializer->getBackendId());
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::buildOmniMainMetaInfo StreamingJoin requires VoidNamespace for state=" +
            flinkMetaInfo.getName());
    }

    omnistream::RowType rowType(true, sidePlan.inputTypeNames);
    TypeSerializer* joinValueSerializer = sidePlan.outerJoinState
                                              ? static_cast<TypeSerializer*>(new JoinTupleSerializer())
                                              : static_cast<TypeSerializer*>(new IntSerializer());
    auto* stateSerializer = new MapSerializer(new RowDataSerializer(&rowType), joinValueSerializer);

    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap.emplace(
        StateMetaInfoSnapshot::commonOptionsKeyToString(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE),
        std::to_string(static_cast<int>(StateDescriptor::Type::MAP)));
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer);
    serializerMap.emplace(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, stateSerializer);

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return StateMetaInfoSnapshot(
        flinkMetaInfo.getName(),
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}

RestoreStateType StreamingJoinSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo) const
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    }
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
        const std::string& stateName = metaInfo.getName();
        if (stateName != StreamingJoinSavepointUtil::LEFT_STATE_NAME &&
            stateName != StreamingJoinSavepointUtil::RIGHT_STATE_NAME) {
            return RestoreStateType::KV;
        }
        return RestoreStateType::KV_TRANSFORMED;
    }
    return RestoreStateType::UNSUPPORT;
}

std::vector<VectorBatchSaveStateContext> StreamingJoinSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(snapshotResources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        if (spec.sourceKvStateId < 0 || static_cast<size_t>(spec.sourceKvStateId) >= contexts.size()) {
            ERROR_RELEASE(
                "StreamingJoinSavepointAdaptor::buildSaveStateContexts ->"
                << " sourceKvStateId=" << spec.sourceKvStateId << ", contextCount=" << contexts.size()
                << ", logicalStateName=" << spec.logicalStateName);
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::buildSaveStateContexts invalid source kvStateId=" +
                std::to_string(spec.sourceKvStateId));
        }
        auto& ctx = contexts[spec.sourceKvStateId];
        ctx.writable = true;
        ctx.mappedKvStateId = plan.kvStateIdMapping.at(spec.sourceKvStateId);
        ctx.logicalStateName = spec.logicalStateName;
        ctx.valueSerializer = spec.valueSerializer;
        ctx.stateType = spec.stateType;
        if (ctx.stateType != VectorBatchStateType::KV_TRANSFORM) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor requires KV_TRANSFORM context for state=" + spec.logicalStateName);
        }
    }
    return contexts;
}

template <typename Emit>
void StreamingJoinSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan& /*plan*/,
    Emit&& output)
{
    if (context.stateType != VectorBatchStateType::KV_TRANSFORM || context.vbAccessor != nullptr) {
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor invalid non-VB conversion context for state=" + context.logicalStateName);
    }

    const SidePlan& sidePlan = sidePlanForState(context.logicalStateName);
    parseSourceMapEntries(entry, sidePlan, [&](ByteView keyBytes, ByteView valueBytes) {
        ConvertedEntry converted;
        converted.context = &context;
        converted.keyBytes.assign(keyBytes.begin(), keyBytes.end());
        converted.valueBytes.assign(valueBytes.begin(), valueBytes.end());
        output(std::move(converted));
    });
}

template <typename Emit>
void StreamingJoinSavepointAdaptor::parseSourceMapEntries(
    const KeyValueStateIterator::CurrentEntry& entry, const SidePlan& sidePlan, Emit&& emit) const
{
    const size_t singleValueSize = 1 + sizeof(int32_t) + (sidePlan.outerJoinState ? sizeof(int32_t) : 0);
    if (entry.value.size() == singleValueSize) {
        StreamingJoinSavepointUtil::parseFlinkJoinValue(entry.value, sidePlan.outerJoinState);
        if (entry.key.empty()) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries empty expanded key, state=" + sidePlan.stateName);
        }
        emit(
            ByteView::fromBuffer(entry.key.data(), entry.key.size()),
            ByteView::fromBuffer(entry.value.data(), entry.value.size()));
        return;
    }

    if (entry.value.size() < sizeof(int32_t)) {
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseSourceMapEntries invalid aggregated value, state=" +
            sidePlan.stateName);
    }

    DataInputDeserializer input(entry.value.data(), static_cast<int>(entry.value.size()), 0);
    int32_t entryCount = input.readInt();
    if (entryCount < 0) {
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseSourceMapEntries negative entry count, state=" + sidePlan.stateName);
    }
    const int32_t minimumEntryBytes =
        static_cast<int32_t>(sizeof(int32_t) + 1 + sizeof(int32_t) + (sidePlan.outerJoinState ? sizeof(int32_t) : 0));
    if (entryCount > 0 && entryCount > input.Available() / minimumEntryBytes) {
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseSourceMapEntries entry count exceeds payload, state=" +
            sidePlan.stateName);
    }

    for (int32_t index = 0; index < entryCount; ++index) {
        if (input.Available() < static_cast<int32_t>(sizeof(int32_t))) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries truncated RowData length, state=" +
                sidePlan.stateName + ", entryIndex=" + std::to_string(index));
        }
        const size_t rowStart = input.getPosition();
        int32_t rowLength = input.readInt();
        if (rowLength <= 0 || rowLength > input.Available()) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries invalid RowData length, state=" +
                sidePlan.stateName + ", entryIndex=" + std::to_string(index));
        }
        input.setPosition(input.getPosition() + static_cast<size_t>(rowLength));
        const size_t rowEnd = input.getPosition();

        const int32_t serializedValueBytes = 1 + static_cast<int32_t>(sizeof(int32_t)) +
                                             (sidePlan.outerJoinState ? static_cast<int32_t>(sizeof(int32_t)) : 0);
        if (input.Available() < serializedValueBytes) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries truncated Join value, state=" +
                sidePlan.stateName + ", entryIndex=" + std::to_string(index));
        }
        const size_t valueStart = input.getPosition();
        if (input.readBoolean()) {
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::parseSourceMapEntries null Join value, state=" + sidePlan.stateName);
        }
        input.readInt();
        if (sidePlan.outerJoinState) {
            input.readInt();
        }
        const size_t valueEnd = input.getPosition();

        std::vector<int8_t> keyBytes;
        keyBytes.reserve(entry.key.size() + rowEnd - rowStart);
        keyBytes.insert(keyBytes.end(), entry.key.begin(), entry.key.end());
        keyBytes.insert(
            keyBytes.end(),
            entry.value.begin() + static_cast<std::ptrdiff_t>(rowStart),
            entry.value.begin() + static_cast<std::ptrdiff_t>(rowEnd));
        ByteView valueBytes = ByteView::fromBuffer(entry.value.data() + valueStart, valueEnd - valueStart);
        emit(ByteView::fromBuffer(keyBytes.data(), keyBytes.size()), valueBytes);
    }

    if (input.Available() != 0) {
        throw std::runtime_error(
            "StreamingJoinSavepointAdaptor::parseSourceMapEntries trailing bytes, state=" + sidePlan.stateName);
    }
}

VectorBatchSavePlan StreamingJoinSavepointAdaptor::buildSavePlan(FullSnapshotResources& snapshotResources)
{
    VectorBatchSavePlan plan;
    const auto& metaInfos = snapshotResources.getMetaInfoSnapshots();

    for (size_t i = 0; i < metaInfos.size(); ++i) {
        const auto& meta = metaInfos[i];
        if (meta == nullptr || VectorBatchSaveTools::isVbStateName(meta->getName())) {
            continue;
        }
        const std::string& stateName = meta->getName();
        const SidePlan& sidePlan = sidePlanForState(stateName);
        int mappedKvStateId = static_cast<int>(plan.targetMetaInfos.size());
        plan.kvStateIdMapping[static_cast<int>(i)] = mappedKvStateId;
        plan.targetMetaInfos.push_back(
            StreamingJoinSavepointUtil::createFlinkMapStateSnapshot(
                stateName, *meta, sidePlan.inputTypeNames, sidePlan.outerJoinState));
        plan.mainStateIds.push_back(static_cast<int>(i));

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = stateName;
        spec.valueSerializer = meta->getValueSerializer();
        if (spec.valueSerializer == nullptr) {
            ERROR_RELEASE(
                "StreamingJoinSavepointAdaptor::buildSavePlan ->"
                << " stateName=" << stateName << ", sourceKvStateId=" << i << ", valueSerializer=null");
            throw std::runtime_error(
                "StreamingJoinSavepointAdaptor::buildSavePlan missing source value serializer for state=" + stateName);
        }
        spec.stateType = VectorBatchStateType::KV_TRANSFORM;
        plan.stateContextSpecs.push_back(std::move(spec));
    }
    return plan;
}

const StreamingJoinSavepointAdaptor::SidePlan& StreamingJoinSavepointAdaptor::sidePlanForState(
    const std::string& stateName) const
{
    if (stateName == StreamingJoinSavepointUtil::LEFT_STATE_NAME) {
        return leftPlan_;
    }
    if (stateName == StreamingJoinSavepointUtil::RIGHT_STATE_NAME) {
        return rightPlan_;
    }
    ERROR_RELEASE(
        "StreamingJoinSavepointAdaptor::sidePlanForState ->"
        << " stateName=" << stateName << ", leftState=" << StreamingJoinSavepointUtil::LEFT_STATE_NAME
        << ", rightState=" << StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
    throw std::runtime_error("StreamingJoinSavepointAdaptor::sidePlanForState unsupported state=" + stateName);
}

} // namespace omnistream
