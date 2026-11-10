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

#include "GroupAggSavepointAdaptor.h"

#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "StateMetaInfoValidator.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/types/logical/RowType.h"

namespace omnistream {

void GroupAggSavepointAdaptor::prepareForSave(const nlohmann::json& description)
{
    prepareAccumulatorTypes(description);

    // RowDataSerializer does not manage the RowType pointer so we should free the temporary RowType after the
    // RowDataSerializer created.
    auto flinkAccRowType = std::make_unique<RowType>(true, flinkAccTypes_);
    auto omniAccRowType = std::make_unique<RowType>(true, omniAccTypes_);

    flinkAccSerializer_ = std::make_unique<RowDataSerializer>(flinkAccRowType.get());
    omniAccSerializer_ = std::make_unique<RowDataSerializer>(omniAccRowType.get());
}

void GroupAggSavepointAdaptor::prepareForRestore(const nlohmann::json& description)
{
    prepareAccumulatorTypes(description);
    auto omniAccRowType = std::make_unique<RowType>(true, omniAccTypes_);
    omniAccSerializer_ = std::make_unique<RowDataSerializer>(omniAccRowType.get());
}

void GroupAggSavepointAdaptor::prepareAccumulatorTypes(const nlohmann::json& description)
{
    if (!description.contains("aggInfoList") || !description["aggInfoList"].is_object()) {
        ERROR_RELEASE("The description does not include aggInfoList, or aggInfoList is not an object.");
        throw std::runtime_error("GroupAggSavepointAdaptor: missing aggInfoList");
    }
    flinkAccTypes_ = parseStringArray(description["aggInfoList"], "accTypes");
    omniAccTypes_.clear();
    flinkToOmniIndex_.assign(flinkAccTypes_.size(), -1);
    for (size_t i = 0; i < flinkAccTypes_.size(); ++i) {
        if (flinkAccTypes_[i].find("RAW") != std::string::npos) {
            continue;
        }
        flinkToOmniIndex_[i] = static_cast<int>(omniAccTypes_.size());
        omniAccTypes_.push_back(flinkAccTypes_[i]);
    }
    flinkAccTypeIds_.resize(flinkAccTypes_.size());
    for (size_t i = 0; i < flinkAccTypes_.size(); ++i) {
        flinkAccTypeIds_[i] = LogicalType::flinkTypeToOmniTypeId(flinkAccTypes_[i]);
    }
}

void GroupAggSavepointAdaptor::validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    // TODO: 状态后端停止注册 vb 侧表后，删除保存侧对可选侧表的接纳。
    validator.requireKeyedValueStateWithVB(ACC_STATE_NAME);
    validator.requirePriorityQueueStates();
    validator.consumeAllKeyedStatesWithPrefix("distinctAcc");
    validator.requireNoMoreStates();
}

void GroupAggSavepointAdaptor::validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
{
    StateMetaInfoValidator validator{metaInfos};
    validator.requireKeyedValueState(ACC_STATE_NAME);
    validator.requirePriorityQueueStates();
    validator.consumeAllKeyedStatesWithPrefix("distinctAcc");
    validator.requireNoMoreStates();
}

VectorBatchSavePlan GroupAggSavepointAdaptor::buildSavePlan(FullSnapshotResources& resources)
{
    VectorBatchSavePlan plan;
    const auto& metas = resources.getMetaInfoSnapshots();
    plan.keyGroupRange = resources.getKeyGroupRange();

    int targetId = 0;
    for (size_t i = 0; i < metas.size(); ++i) {
        const auto& meta = metas[i];
        if (meta == nullptr) {
            continue;
        }
        // TODO: 状态后端停止注册 vb 侧表后，删除保存侧的侧表过滤。
        if (VectorBatchSaveTools::isVbStateName(meta->getName())) {
            continue;
        }
        if (meta->getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE &&
            meta->getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            ERROR_RELEASE("The backend is not KEY_VALUE and PRIORITY_QUEUE.");
            throw std::runtime_error("GroupAggSavepointAdaptor: unsupported backend state type for " + meta->getName());
        }
        plan.kvStateIdMapping[static_cast<int>(i)] = targetId++;
        plan.mainStateIds.push_back(static_cast<int>(i));

        VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = static_cast<int>(i);
        spec.logicalStateName = meta->getName();
        spec.stateType = meta->getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE
                             ? VectorBatchStateType::PQ
                             : VectorBatchStateType::KV;

        if (meta->getName() == ACC_STATE_NAME) {
            TypeSerializer* namespaceSerializer = meta->getNamespaceSerializer();
            if (namespaceSerializer == nullptr || flinkAccSerializer_ == nullptr || omniAccSerializer_ == nullptr) {
                ERROR_RELEASE("The namespaceSerializer, flinkAccSerializer_, or omniAccSerializer_ is null.");
                throw std::runtime_error("GroupAggSavepointAdaptor: missing accState serializer");
            }
            RegisteredKeyValueStateBackendMetaInfo flinkMeta(
                StateDescriptor::Type::VALUE, ACC_STATE_NAME, namespaceSerializer, flinkAccSerializer_.get());
            plan.targetMetaInfos.push_back(flinkMeta.snapshot());
            spec.stateType = VectorBatchStateType::KV_TRANSFORM;
            spec.valueSerializer = flinkAccSerializer_.get();
            // Restored metadata only keeps non-owning serializer pointers. The restore adaptor that created the
            // accState metadata has already been destroyed by the time a later compatible save starts, so use the
            // serializer owned by this save adaptor instead of dereferencing the metadata pointer.
            spec.sourceValueSerializer = omniAccSerializer_.get();
        } else {
            plan.targetMetaInfos.push_back(meta);
            spec.valueSerializer = meta->getValueSerializer();
        }
        plan.stateContextSpecs.push_back(std::move(spec));
    }
    return plan;
}

std::vector<VectorBatchSaveStateContext> GroupAggSavepointAdaptor::buildSaveStateContexts(
    FullSnapshotResources& resources, const VectorBatchSavePlan& plan)
{
    std::vector<VectorBatchSaveStateContext> contexts(resources.getMetaInfoSnapshots().size());
    for (const auto& spec : plan.stateContextSpecs) {
        auto& ctx = contexts.at(static_cast<size_t>(spec.sourceKvStateId));
        ctx.writable = true;
        ctx.mappedKvStateId = plan.kvStateIdMapping.at(spec.sourceKvStateId);
        ctx.logicalStateName = spec.logicalStateName;
        ctx.stateType = spec.stateType;
        ctx.valueSerializer = spec.valueSerializer;
        ctx.sourceValueSerializer = spec.sourceValueSerializer;
    }
    return contexts;
}

std::unique_ptr<BinaryRowData> GroupAggSavepointAdaptor::expandAccumulator(RowData& source) const
{
    if (source.getArity() != static_cast<int>(omniAccTypes_.size())) {
        ERROR_RELEASE("Omni accumulator arity does not match non-RAW accTypes.");
        throw std::runtime_error("GroupAggSavepointAdaptor: Omni accumulator arity does not match non-RAW accTypes");
    }
    std::unique_ptr<BinaryRowData> target(BinaryRowData::createBinaryRowDataWithMem(flinkAccTypes_.size()));
    target->setRowKind(source.getRowKind());
    for (size_t flinkIndex = 0; flinkIndex < flinkAccTypes_.size(); ++flinkIndex) {
        int omniIndex = flinkToOmniIndex_[flinkIndex];
        if (omniIndex < 0 || source.isNullAt(omniIndex)) {
            target->setNullAt(static_cast<int>(flinkIndex));
            continue;
        }
        switch (flinkAccTypeIds_[flinkIndex]) {
            case omniruntime::type::DataTypeId::OMNI_INT:
                target->setInt(static_cast<int>(flinkIndex), *source.getInt(omniIndex));
                break;
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            case omniruntime::type::DataTypeId::OMNI_CHAR:
                target->setStringView(static_cast<int>(flinkIndex), source.getStringView(omniIndex));
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                target->setLong(static_cast<int>(flinkIndex), *source.getLong(omniIndex));
                break;
            default:
                ERROR_RELEASE("The current accTypes are not supported.");
                throw std::runtime_error(
                    "GroupAggSavepointAdaptor: unsupported non-RAW accumulator type " + flinkAccTypes_[flinkIndex]);
        }
    }
    return target;
}

std::unique_ptr<BinaryRowData> GroupAggSavepointAdaptor::compactAccumulator(RowData& source) const
{
    if (source.getArity() != static_cast<int>(flinkAccTypes_.size())) {
        ERROR_RELEASE("Flink accumulator arity does not match accTypes.");
        throw std::runtime_error("GroupAggSavepointAdaptor: Flink accumulator arity does not match accTypes");
    }
    std::unique_ptr<BinaryRowData> target(BinaryRowData::createBinaryRowDataWithMem(omniAccTypes_.size()));
    target->setRowKind(source.getRowKind());
    for (size_t flinkIndex = 0; flinkIndex < flinkAccTypes_.size(); ++flinkIndex) {
        int omniIndex = flinkToOmniIndex_[flinkIndex];
        if (omniIndex < 0) {
            // Flink 的 accState 中 DataView(RAW)字段是非 null 的:codegen 在 setAccumulators 时
            // 会把包装了 StateMapView/StateListView 的 BinaryRawValueData 设置进 accumulator,
            // Flink 恢复时也会重新绑定当前 operator 的 StateDataView 覆盖该字段,
            // 真实 DataView 数据在独立 keyed state(distinct_acc 等)中单独恢复。
            // 因此这里直接丢弃 RAW 字段内容,无论其是否为 null。
            continue;
        }
        if (source.isNullAt(static_cast<int>(flinkIndex))) {
            target->setNullAt(omniIndex);
            continue;
        }
        switch (flinkAccTypeIds_[flinkIndex]) {
            case omniruntime::type::DataTypeId::OMNI_INT:
                target->setInt(omniIndex, *source.getInt(static_cast<int>(flinkIndex)));
                break;
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            case omniruntime::type::DataTypeId::OMNI_CHAR:
                target->setStringView(omniIndex, source.getStringView(static_cast<int>(flinkIndex)));
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                target->setLong(omniIndex, *source.getLong(static_cast<int>(flinkIndex)));
                break;
            default:
                ERROR_RELEASE("The current accTypes are not supported.");
                throw std::runtime_error(
                    "GroupAggSavepointAdaptor: unsupported non-RAW accumulator type " + flinkAccTypes_[flinkIndex]);
        }
    }
    return target;
}

template <typename Emit>
void GroupAggSavepointAdaptor::convertKVRowData(
    const KeyValueStateIterator::CurrentEntry& entry,
    const VectorBatchSaveStateContext& context,
    const VectorBatchSavePlan&,
    Emit&& output)
{
    if (context.stateType != VectorBatchStateType::KV_TRANSFORM || context.sourceValueSerializer == nullptr) {
        ERROR_RELEASE("The stateType is not KV_TRANSFORM or sourceValueSerializer is null.");
        throw std::runtime_error("GroupAggSavepointAdaptor: invalid transformed state context");
    }
    DataInputDeserializer input(entry.value.data(), static_cast<int>(entry.value.size()), 0);
    auto* source = static_cast<RowData*>(context.sourceValueSerializer->deserialize(input));
    if (source == nullptr) {
        ERROR_RELEASE("The source is null.");
        throw std::runtime_error("GroupAggSavepointAdaptor: failed to deserialize accState");
    }
    auto expanded = expandAccumulator(*source);
    DataOutputSerializer outputSerializer(128);
    context.valueSerializer->serialize(expanded.get(), outputSerializer);

    ConvertedEntry converted;
    converted.context = &context;
    converted.keyBytes.assign(entry.key.begin(), entry.key.end());
    converted.valueBytes.assign(
        outputSerializer.getData(), outputSerializer.getData() + outputSerializer.getPosition());
    output(std::move(converted));
}

void GroupAggSavepointAdaptor::save(
    CheckpointStateOutputStreamProxy& stream,
    KeyGroupRangeOffsets& offsets,
    FullSnapshotResources& resources,
    std::string keySerializer)
{
    auto plan = buildSavePlan(resources);
    VectorBatchSaveFlow::executeSave(*this, plan, stream, offsets, resources, std::move(keySerializer));
}

RestoreStateType GroupAggSavepointAdaptor::getStateType(const StateMetaInfoSnapshot& metaInfo)
{
    if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
        return RestoreStateType::PQ;
    }
    if (metaInfo.getBackendStateType() != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
        return RestoreStateType::UNSUPPORT;
    }
    if (metaInfo.getName() == ACC_STATE_NAME) {
        return RestoreStateType::KV_TRANSFORM;
    }
    return RestoreStateType::KV;
}

StateMetaInfoSnapshot GroupAggSavepointAdaptor::buildOmniMainMetaInfo(
    int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo)
{
    if (flinkMetaInfo.getName() != ACC_STATE_NAME) {
        throw std::runtime_error(
            "GroupAggSavepointAdaptor: buildOmniMainMetaInfo only supports accState, got " + flinkMetaInfo.getName());
    }
    auto* namespaceSerializer = flinkMetaInfo.getNamespaceSerializer();
    auto* sourceSerializer = flinkMetaInfo.getValueSerializer();
    if (namespaceSerializer == nullptr || sourceSerializer == nullptr || omniAccSerializer_ == nullptr) {
        ERROR_RELEASE("The namespaceSerializer is null or sourceSerializer is null or omniAccSerializer_ is null.");
        throw std::runtime_error("GroupAggSavepointAdaptor: missing restore accState serializer");
    }
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        throw std::runtime_error("GroupAggSavepointAdaptor: accState requires VoidNamespaceSerializer");
    }
    sourceSerializers_[kvStateId] = sourceSerializer;
    // 直接构造 StateMetaInfoSnapshot，使用 commonSerializerKeyToString (UPPERCASE) key，
    // 与 fromMetaInfoSnapshot() 的读取 key 保持一致。
    // 避免通过 RegisteredKeyValueStateBackendMetaInfo::computeSnapshot() 间接构造时
    // 因 key 大小写不一致导致序列化器丢失。
    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] =
        std::to_string(static_cast<int>(StateDescriptor::Type::VALUE));

    // The restore adaptor and JNI metadata are temporary, while RocksDB keeps registered metadata for its lifetime.
    // Keep an independent value serializer and use the process-lifetime namespace singleton so neither pointer in
    // the registered accState metadata depends on the source metadata or restore adaptor.
    auto backendValueSerializer = std::make_unique<RowDataSerializer>(new RowType(true, omniAccTypes_));
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace(
        StateMetaInfoSnapshot::commonSerializerKeyToString(
            StateMetaInfoSnapshot::CommonSerializerKeys::NAMESPACE_SERIALIZER),
        VoidNamespaceSerializer::INSTANCE);
    serializerMap.emplace(
        StateMetaInfoSnapshot::commonSerializerKeyToString(
            StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER),
        backendValueSerializer.release());

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return StateMetaInfoSnapshot(
        ACC_STATE_NAME,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}

void GroupAggSavepointAdaptor::transformKVData(
    const std::vector<int8_t>& key, const std::vector<int8_t>& value, int kvStateId, RestoreKVState* writer)
{
    auto it = sourceSerializers_.find(kvStateId);
    if (it == sourceSerializers_.end()) {
        ERROR_RELEASE("Cannot find kvStateId " << kvStateId << " .");
        throw std::runtime_error(
            "GroupAggSavepointAdaptor: missing source serializer for kvStateId " + std::to_string(kvStateId));
    }
    DataInputDeserializer input(reinterpret_cast<const uint8_t*>(value.data()), static_cast<int>(value.size()), 0);
    auto* flinkAccumulator = static_cast<RowData*>(it->second->deserialize(input));
    if (flinkAccumulator == nullptr) {
        ERROR_RELEASE("The flinkAccumulator is null.");
        throw std::runtime_error("GroupAggSavepointAdaptor: failed to deserialize Flink accumulator");
    }
    auto omniAccumulator = compactAccumulator(*flinkAccumulator);
    DataOutputSerializer output(128);
    omniAccSerializer_->serialize(omniAccumulator.get(), output);
    ByteView valueView(reinterpret_cast<const int8_t*>(output.getData()), output.getPosition());
    writer->writeEntry(key, valueView);
}

void GroupAggSavepointAdaptor::restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
{
    sourceSerializers_.clear();
    VectorBatchRestoreFlow::executeRestore(*this, restoreIterator, backend);
}
} // namespace omnistream
