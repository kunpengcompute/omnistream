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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "core/utils/function/Supplier.h"
#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/JobManagerTaskRestore.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/TaskStateSnapshotDeserializer.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "runtime/jobgraph/JobVertexID.h"
#include "runtime/scheduler/strategy/ExecutionVertexIDPOD.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/TaskLocalStateStore.h"
#include "runtime/state/TaskStateManager.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/bridge/TaskStateManagerBridge.h"
#include "runtime/state/heap/HeapKeyedStateBackendBuilder.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/taskmanager/CheckpointResponder.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/OperatorSnapshotFinalizer.h"
#include "streaming/runtime/io/StreamTaskNetworkOutput.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/runtime/tasks/OperatorChain.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"

namespace {

constexpr int kNumberOfKeyGroups = 8;
constexpr const char* kReduceStateName = "dt-keyed-reduce-state";
constexpr int kEndOfKeyGroupMark = 0xffff;
constexpr int kFirstBitInByteMask = 0x80;
constexpr int kMaxEntriesPerBridgeBatch = 1000;

struct InputRecord {
    int key;
    int value;
};

using SinkRecord = std::pair<int, int>;
using ReduceState = HeapValueState<int, VoidNamespace, int>;

ReduceState* createReduceState(HeapKeyedStateBackend<int>* backend);

class InMemoryOmniStateBridge : public omnistream::OmniTaskBridge {
public:
    void declineCheckpoint(std::string&, std::string&, std::string&) override
    {
    }

    std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
        jlong,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&,
        std::shared_ptr<LocalRecoveryConfig>,
        CheckpointOptions*,
        std::string) override
    {
        return SnapshotResult<StreamStateHandle>::Empty();
    }

    jobject CallUploadFilesToCheckpointFs(const std::vector<Path>&, int) override
    {
        return nullptr;
    }

    std::vector<StateMetaInfoSnapshot> readMetaData(const std::string& serializedHandle) override
    {
        return normalizeMetaInfoSnapshots(*snapshotsForHandle(serializedHandle));
    }

    std::vector<StateMetaInfoSnapshot> readOperatorMetaData(const std::string&) override
    {
        return {};
    }

    jobject AcquireSavepointOutputStream(long checkpointId, CheckpointOptions*) override
    {
        auto* stream = new OutputStream();
        stream->checkpointId = checkpointId;
        stream->handleName =
            "heap-state-backend-dt-" + std::to_string(checkpointId) + "-" + std::to_string(nextHandleOrdinal_++);
        return reinterpret_cast<jobject>(stream);
    }

    std::shared_ptr<SnapshotResult<StreamStateHandle>> CloseSavepointOutputStream(jobject provider) override
    {
        auto* stream = outputStream(provider);
        latestData_ = stream->data;
        latestMetaInfoSnapshots_ = stream->metaInfoSnapshots;
        dataByHandleName_[stream->handleName] = stream->data;
        metaInfoSnapshotsByHandleName_[stream->handleName] = stream->metaInfoSnapshots;
        auto handle = std::make_shared<ByteStreamStateHandle>(stream->handleName, stream->data);
        delete stream;
        return SnapshotResult<StreamStateHandle>::Of(handle);
    }

    void AbortSavepointOutputStream(jobject provider) override
    {
        delete outputStream(provider);
    }

    void WriteSavepointOutputStream(jobject provider, const int8_t* chunk, size_t offset, size_t len) override
    {
        auto* stream = outputStream(provider);
        auto* begin = reinterpret_cast<const uint8_t*>(chunk + offset);
        stream->data.insert(stream->data.end(), begin, begin + len);
    }

    jobject CreateSavepointOutputDirectBuffer(void* data, size_t) override
    {
        return reinterpret_cast<jobject>(data);
    }

    void ReleaseSavepointOutputDirectBuffer(jobject) override
    {
    }

    bool WriteSavepointOutputStreamDirect(jobject provider, jobject directBuffer, size_t len) override
    {
        auto* stream = outputStream(provider);
        auto* begin = reinterpret_cast<const uint8_t*>(directBuffer);
        stream->data.insert(stream->data.end(), begin, begin + len);
        return true;
    }

    void WriteSavepointMetadata(
        jobject provider, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots, std::string) override
    {
        auto* stream = outputStream(provider);
        stream->metaInfoSnapshots = snapshots;

        // Real Java writes metadata into the same stream before key-group data.
        // Keep offsets non-zero so KeyGroupEntryIterator exercises the restore path.
        if (stream->data.empty()) {
            stream->data.push_back(0);
        }
    }

    void WriteOperatorMetaData(
        jobject,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&) override
    {
    }

    long GetSavepointOutputStreamPos(jobject provider) override
    {
        return static_cast<long>(outputStream(provider)->data.size());
    }

    void getKeyGroupEntries(
        jobject inputStream, int& currentKvStateId, bool, std::vector<KeyGroupEntry>& entries) override
    {
        auto* stream = restoreInputStream(inputStream);
        entries.clear();

        if (currentKvStateId == -1) {
            if (!canRead(*stream, sizeof(int16_t))) {
                currentKvStateId = kEndOfKeyGroupMark;
                return;
            }
            currentKvStateId = readUnsignedShort(*stream);
        }

        if ((currentKvStateId & kEndOfKeyGroupMark) == kEndOfKeyGroupMark) {
            return;
        }

        const int entryStateId = currentKvStateId;
        for (int i = 0; i < kMaxEntriesPerBridgeBatch; i++) {
            if (!canRead(*stream, sizeof(int32_t))) {
                currentKvStateId = kEndOfKeyGroupMark;
                return;
            }

            std::vector<int8_t> key = readByteArray(*stream);
            std::vector<int8_t> value = readByteArray(*stream);
            const bool metadataFollows = !key.empty() && ((static_cast<uint8_t>(key[0]) & kFirstBitInByteMask) != 0);
            if (metadataFollows) {
                key[0] = static_cast<int8_t>(static_cast<uint8_t>(key[0]) & ~static_cast<uint8_t>(kFirstBitInByteMask));
                currentKvStateId = readUnsignedShort(*stream);
                entries.emplace_back(entryStateId, std::move(key), std::move(value));
                return;
            }
            entries.emplace_back(entryStateId, std::move(key), std::move(value));
        }
    }

    jobject getSavepointInputStream(const std::string& serializedHandle) override
    {
        auto* stream = new InputStream();
        const std::string handleName = handleNameFromSerializedStateHandle(serializedHandle);
        auto it = dataByHandleName_.find(handleName);
        stream->data = it == dataByHandleName_.end() ? latestData_ : it->second;
        stream->pos = 0;
        return reinterpret_cast<jobject>(stream);
    }

    void setSavepointInputStreamOffset(jobject inputStream, int64_t offset) override
    {
        auto* stream = restoreInputStream(inputStream);
        if (offset < 0 || static_cast<size_t>(offset) > stream->data.size()) {
            throw std::out_of_range("restore input stream offset out of bounds");
        }
        stream->pos = static_cast<size_t>(offset);
    }

    int ReadSavepointInputStream(jobject inputStream, int8_t* chunk, size_t offset, size_t len) override
    {
        auto* stream = restoreInputStream(inputStream);
        if (stream->pos >= stream->data.size()) {
            return -1;
        }
        const size_t readable = std::min(len, stream->data.size() - stream->pos);
        for (size_t i = 0; i < readable; i++) {
            chunk[offset + i] = static_cast<int8_t>(stream->data[stream->pos + i]);
        }
        stream->pos += readable;
        return static_cast<int>(readable);
    }

    bool isUsingKeyGroupCompression(jobject) override
    {
        return false;
    }

    void closeSavepointInputStream(jobject inputStream) override
    {
        delete restoreInputStream(inputStream);
    }

    JNIEnv* getJNIEnv() override
    {
        return nullptr;
    }

    bool CallDownloadFileToLocal(const StreamStateHandle&, const std::string&) override
    {
        return false;
    }

private:
    struct OutputStream {
        long checkpointId = 0;
        std::string handleName;
        std::vector<uint8_t> data;
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfoSnapshots;
    };

    struct InputStream {
        std::vector<uint8_t> data;
        size_t pos = 0;
    };

    std::unordered_map<std::string, std::vector<uint8_t>> dataByHandleName_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>> metaInfoSnapshotsByHandleName_;
    std::vector<uint8_t> latestData_;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> latestMetaInfoSnapshots_;
    int nextHandleOrdinal_ = 0;

    static std::vector<StateMetaInfoSnapshot> normalizeMetaInfoSnapshots(
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots)
    {
        std::vector<StateMetaInfoSnapshot> normalized;
        normalized.reserve(snapshots.size());
        for (const auto& snapshot : snapshots) {
            std::unordered_map<std::string, TypeSerializer*> serializers;
            TypeSerializer* namespaceSerializer =
                firstSerializer(snapshot.get(), "namespaceSerializer", "NAMESPACE_SERIALIZER");
            TypeSerializer* stateSerializer = firstSerializer(snapshot.get(), "stateSerializer", "VALUE_SERIALIZER");
            if (namespaceSerializer != nullptr) {
                serializers.emplace("NAMESPACE_SERIALIZER", namespaceSerializer);
            }
            if (stateSerializer != nullptr) {
                serializers.emplace("VALUE_SERIALIZER", stateSerializer);
            }

            normalized.emplace_back(
                snapshot->getName(),
                snapshot->getBackendStateType(),
                snapshot->getOptionsImmutable(),
                snapshot->getSerializerSnapshotsImmutable(),
                serializers);
        }
        return normalized;
    }

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>* snapshotsForHandle(const std::string& serializedHandle)
    {
        const std::string handleName = handleNameFromSerializedStateHandle(serializedHandle);
        auto it = metaInfoSnapshotsByHandleName_.find(handleName);
        return it == metaInfoSnapshotsByHandleName_.end() ? &latestMetaInfoSnapshots_ : &it->second;
    }

    static std::string handleNameFromSerializedStateHandle(const std::string& serializedHandle)
    {
        if (serializedHandle.empty()) {
            return "";
        }
        try {
            return handleNameFromJson(nlohmann::json::parse(serializedHandle));
        } catch (...) {
            return "";
        }
    }

    static std::string handleNameFromJson(const nlohmann::json& json)
    {
        if (!json.is_object()) {
            return "";
        }
        auto handleName = json.find("handleName");
        if (handleName != json.end() && handleName->is_string()) {
            return handleName->get<std::string>();
        }
        for (const char* childName : {"streamStateHandle", "stateHandle", "delegateStateHandle"}) {
            auto child = json.find(childName);
            if (child == json.end() || child->is_null()) {
                continue;
            }
            std::string nestedHandleName = handleNameFromJson(*child);
            if (!nestedHandleName.empty()) {
                return nestedHandleName;
            }
        }
        return "";
    }

    static TypeSerializer* firstSerializer(
        const StateMetaInfoSnapshot* snapshot, const std::string& primary, const std::string& fallback)
    {
        TypeSerializer* serializer = snapshot->getTypeSerializer(primary);
        return serializer != nullptr ? serializer : snapshot->getTypeSerializer(fallback);
    }

    static OutputStream* outputStream(jobject provider)
    {
        if (provider == nullptr) {
            throw std::runtime_error("null savepoint output stream");
        }
        return reinterpret_cast<OutputStream*>(provider);
    }

    static InputStream* restoreInputStream(jobject inputStream)
    {
        if (inputStream == nullptr) {
            throw std::runtime_error("null savepoint input stream");
        }
        return reinterpret_cast<InputStream*>(inputStream);
    }

    static bool canRead(const InputStream& stream, size_t len)
    {
        return stream.pos <= stream.data.size() && len <= stream.data.size() - stream.pos;
    }

    static uint8_t readByte(InputStream& stream)
    {
        if (!canRead(stream, 1)) {
            throw std::out_of_range("unexpected end of savepoint input stream");
        }
        return stream.data[stream.pos++];
    }

    static int readUnsignedShort(InputStream& stream)
    {
        int value = (static_cast<int>(readByte(stream)) << 8);
        value |= static_cast<int>(readByte(stream));
        return value & kEndOfKeyGroupMark;
    }

    static int readInt(InputStream& stream)
    {
        uint32_t value = (static_cast<uint32_t>(readByte(stream)) << 24);
        value |= (static_cast<uint32_t>(readByte(stream)) << 16);
        value |= (static_cast<uint32_t>(readByte(stream)) << 8);
        value |= static_cast<uint32_t>(readByte(stream));
        return static_cast<int>(value);
    }

    static std::vector<int8_t> readByteArray(InputStream& stream)
    {
        const int len = readInt(stream);
        if (len < 0 || !canRead(stream, static_cast<size_t>(len))) {
            throw std::out_of_range("invalid savepoint byte-array length");
        }
        std::vector<int8_t> result(static_cast<size_t>(len));
        for (int i = 0; i < len; i++) {
            result[static_cast<size_t>(i)] = static_cast<int8_t>(readByte(stream));
        }
        return result;
    }
};

struct HeapBackendReduceHarness {
    explicit HeapBackendReduceHarness(std::shared_ptr<InMemoryOmniStateBridge> bridge = nullptr)
        : keyGroupRange(new KeyGroupRange(0, kNumberOfKeyGroups - 1)),
          keyContext(new InternalKeyContextImpl<int>(keyGroupRange, kNumberOfKeyGroups)),
          backend(new HeapKeyedStateBackend<int>(new IntSerializer(), keyContext))
    {
        if (bridge != nullptr) {
            backend->setOmniTaskBridge(bridge);
        }
        state = createReduceState(backend);
    }

    HeapBackendReduceHarness(KeyGroupRange* restoredRange, HeapKeyedStateBackend<int>* restoredBackend)
        : keyGroupRange(restoredRange),
          keyContext(nullptr),
          backend(restoredBackend)
    {
        state = createReduceState(backend);
    }

    ~HeapBackendReduceHarness()
    {
        delete backend;
        delete keyContext;
        delete keyGroupRange;
    }

    HeapBackendReduceHarness(const HeapBackendReduceHarness&) = delete;
    HeapBackendReduceHarness& operator=(const HeapBackendReduceHarness&) = delete;

    KeyGroupRange* keyGroupRange;
    InternalKeyContextImpl<int>* keyContext;
    HeapKeyedStateBackend<int>* backend;
    ReduceState* state;
};

ReduceState* createReduceState(HeapKeyedStateBackend<int>* backend)
{
    auto* descriptor = new ValueStateDescriptor<int>(kReduceStateName, new IntSerializer());
    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto* state = reinterpret_cast<ReduceState*>(stateHandle);
    if (state == nullptr) {
        throw std::runtime_error("failed to create reduce value state");
    }
    return state;
}

int processThroughSourceMapReduceMap(HeapBackendReduceHarness& harness, const InputRecord& record)
{
    const int mappedBeforeReduce = record.value * 2;
    harness.backend->setCurrentKey(record.key);
    const int reducedValue = harness.state->value() + mappedBeforeReduce;
    harness.state->update(reducedValue);
    return reducedValue + 1000;
}

std::vector<SinkRecord> processRecords(HeapBackendReduceHarness& harness, const std::vector<InputRecord>& records)
{
    std::vector<SinkRecord> sink;
    sink.reserve(records.size());
    for (const auto& record : records) {
        sink.emplace_back(record.key, processThroughSourceMapReduceMap(harness, record));
    }
    return sink;
}

std::vector<int> readKeyedReduceState(HeapBackendReduceHarness& harness, const std::vector<int>& keys)
{
    std::vector<int> values;
    values.reserve(keys.size());
    for (int key : keys) {
        harness.backend->setCurrentKey(key);
        values.push_back(harness.state->value());
    }
    return values;
}

std::shared_ptr<KeyedStateHandle> snapshotHeapBackend(
    HeapKeyedStateBackend<int>* backend, long checkpointId, CheckpointOptions* checkpointOptions)
{
    auto task = backend->snapshot(checkpointId, 0, nullptr, checkpointOptions);
    auto future = task->get_future();
    (*task)();
    auto result = future.get();
    if (result == nullptr) {
        throw std::runtime_error("snapshot returned null result");
    }
    auto handle = result->GetJobManagerOwnedSnapshot();
    if (handle == nullptr) {
        throw std::runtime_error("snapshot returned empty keyed state handle");
    }
    return handle;
}

std::unique_ptr<HeapBackendReduceHarness> restoreHeapBackend(
    const std::shared_ptr<InMemoryOmniStateBridge>& bridge, const std::shared_ptr<KeyedStateHandle>& handle)
{
    auto* range = new KeyGroupRange(0, kNumberOfKeyGroups - 1);
    std::set<std::shared_ptr<KeyedStateHandle>> handles;
    handles.insert(handle);

    HeapKeyedStateBackendBuilder<int> builder(new IntSerializer(), kNumberOfKeyGroups, range);
    HeapKeyedStateBackend<int>* restoredBackend = builder.setOmniTaskBridge(bridge).setStateHandles(handles).build();

    return std::make_unique<HeapBackendReduceHarness>(range, restoredBackend);
}

void assertBackendSnapshotRestoreKeepsReduceStateConsistent(CheckpointOptions* checkpointOptions)
{
    const std::vector<InputRecord> beforeSnapshot = {{1, 3}, {2, 4}, {1, 5}, {9, 7}};
    const std::vector<InputRecord> afterRestore = {{2, 6}, {1, 8}, {3, 10}, {9, 1}};
    const std::vector<int> observedKeys = {1, 2, 3, 9};

    HeapBackendReduceHarness baseline;
    processRecords(baseline, beforeSnapshot);
    const std::vector<SinkRecord> expectedTailSink = processRecords(baseline, afterRestore);
    const std::vector<int> expectedFinalState = readKeyedReduceState(baseline, observedKeys);

    auto bridge = std::make_shared<InMemoryOmniStateBridge>();
    HeapBackendReduceHarness beforeFailure(bridge);
    processRecords(beforeFailure, beforeSnapshot);
    const std::vector<int> stateBeforeSnapshot = readKeyedReduceState(beforeFailure, observedKeys);
    auto stateHandle = snapshotHeapBackend(beforeFailure.backend, 42, checkpointOptions);

    ASSERT_NE(std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(stateHandle), nullptr);

    auto restored = restoreHeapBackend(bridge, stateHandle);
    const std::vector<int> stateAfterRestore = readKeyedReduceState(*restored, observedKeys);
    const std::vector<SinkRecord> actualTailSink = processRecords(*restored, afterRestore);
    const std::vector<int> actualFinalState = readKeyedReduceState(*restored, observedKeys);

    EXPECT_EQ(stateAfterRestore, stateBeforeSnapshot);
    EXPECT_EQ(actualTailSink, expectedTailSink);
    EXPECT_EQ(actualFinalState, expectedFinalState);
}

constexpr const char* kSourceMapOperatorId = "11111111111111111111111111111111";
constexpr const char* kReduceOperatorId = "22222222222222222222222222222222";
constexpr const char* kSinkMapOperatorId = "33333333333333333333333333333333";
constexpr const char* kOperatorChainReduceStateName = "dt-operator-chain-keyed-reduce-state";
constexpr const char* kHeapBackendTaskStateBackend = "HashMapStateBackend";

struct ChainRecord {
    long key;
    long value;
};

class NoLocalStateTaskStateManagerBridge : public omnistream::TaskStateManagerBridge {
public:
    void ReportTaskStateSnapshots(std::string&, std::string&, std::string&, std::string&) override
    {
    }

    void notifyCheckpointAborted(std::string) override
    {
    }

    void NotifyCheckpointComplete(std::string) override
    {
    }

    std::shared_ptr<TaskStateSnapshot> RetrieveLocalState(long) override
    {
        return nullptr;
    }
};

class CollectingSinkOutput : public WatermarkGaugeExposingOutput {
public:
    void collect(void* record) override
    {
        auto* streamRecord = reinterpret_cast<StreamRecord*>(record);
        auto* value = reinterpret_cast<ChainRecord*>(streamRecord->getValue());
        records_.emplace_back(static_cast<int>(value->key), static_cast<int>(value->value));
    }

    void close() override
    {
    }

    void emitWatermark(Watermark*) override
    {
    }

    void emitWatermarkStatus(WatermarkStatus*) override
    {
    }

    const std::vector<SinkRecord>& records() const
    {
        return records_;
    }

private:
    std::vector<SinkRecord> records_;
};

class KeyContextAwareChainingOutput : public WatermarkGaugeExposingOutput {
public:
    explicit KeyContextAwareChainingOutput(OneInputStreamOperator* next) : next_(next)
    {
    }

    void collect(void* record) override
    {
        auto* streamRecord = reinterpret_cast<StreamRecord*>(record);
        if (next_->isSetKeyContextElement()) {
            next_->setKeyContextElement(streamRecord);
        }
        next_->processElement(streamRecord);
    }

    void close() override
    {
    }

    void emitWatermark(Watermark* watermark) override
    {
        next_->ProcessWatermark(watermark);
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        next_->processWatermarkStatus(watermarkStatus);
    }

private:
    OneInputStreamOperator* next_;
};

class MapOperatorForDt : public AbstractStreamOperator<long>, public OneInputStreamOperator {
public:
    MapOperatorForDt(Output* output, std::string name, long multiplier, long addend)
        : AbstractStreamOperator<long>(output),
          name_(std::move(name)),
          multiplier_(multiplier),
          addend_(addend)
    {
        setup();
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        initializedWithKeySerializer_ = keySerializer != nullptr;
        AbstractStreamOperator<long>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<long>::initializeState(initializer, keySerializer);
    }

    void open() override
    {
        opened_ = true;
    }

    void close() override
    {
        AbstractStreamOperator<long>::close();
    }

    void processBatch(StreamRecord* element) override
    {
        processElement(element);
    }

    void processElement(StreamRecord* element) override
    {
        auto* value = reinterpret_cast<ChainRecord*>(element->getValue());
        value->value = value->value * multiplier_ + addend_;
        this->output->collect(element);
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        AbstractStreamOperator<long>::ProcessWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        AbstractStreamOperator<long>::processWatermarkStatus(watermarkStatus);
    }

    bool canBeStreamOperator() override
    {
        return true;
    }

    const char* getName() override
    {
        return name_.c_str();
    }

    std::string getTypeName() override
    {
        return name_;
    }

    bool wasInitializedWithKeySerializer() const
    {
        return initializedWithKeySerializer_;
    }

    bool wasOpened() const
    {
        return opened_;
    }

private:
    std::string name_;
    long multiplier_;
    long addend_;
    bool initializedWithKeySerializer_ = false;
    bool opened_ = false;
};

class HeapKeyedReduceOperatorForDt : public AbstractStreamOperator<long>, public OneInputStreamOperator {
public:
    explicit HeapKeyedReduceOperatorForDt(Output* output) : AbstractStreamOperator<long>(output)
    {
        setup();
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        initializedWithKeySerializer_ = keySerializer != nullptr;
        AbstractStreamOperator<long>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<long>::initializeState(initializer, keySerializer);
    }

    void open() override
    {
        auto* stateDescriptor = new ValueStateDescriptor<long>(kOperatorChainReduceStateName, new LongSerializer());
        values_ = this->stateHandler->getKeyedStateStore()->template getState<long>(stateDescriptor);
        opened_ = true;
    }

    void close() override
    {
        AbstractStreamOperator<long>::close();
    }

    bool isSetKeyContextElement() override
    {
        return true;
    }

    void setKeyContextElement(StreamRecord* record) override
    {
        auto* value = reinterpret_cast<ChainRecord*>(record->getValue());
        this->stateHandler->setCurrentKey(value->key);
    }

    void processBatch(StreamRecord* element) override
    {
        processElement(element);
    }

    void processElement(StreamRecord* element) override
    {
        auto* value = reinterpret_cast<ChainRecord*>(element->getValue());
        const long previous = values_->value();
        const long updated = previous + value->value;
        values_->update(updated);
        value->value = updated;
        this->output->collect(element);
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        AbstractStreamOperator<long>::ProcessWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        AbstractStreamOperator<long>::processWatermarkStatus(watermarkStatus);
    }

    bool canBeStreamOperator() override
    {
        return true;
    }

    const char* getName() override
    {
        return "keyedReduce";
    }

    std::string getTypeName() override
    {
        return "HeapKeyedReduceOperatorForDt";
    }

    long stateValueForKey(long key)
    {
        this->stateHandler->setCurrentKey(key);
        return values_->value();
    }

    bool wasInitializedWithKeySerializer() const
    {
        return initializedWithKeySerializer_;
    }

    bool wasOpened() const
    {
        return opened_;
    }

    bool usesHeapKeyedStateBackend()
    {
        return dynamic_cast<HeapKeyedStateBackend<long>*>(this->stateHandler->getKeyedStateBackend()) != nullptr;
    }

private:
    ValueState<long>* values_ = nullptr;
    bool initializedWithKeySerializer_ = false;
    bool opened_ = false;
};

class ManuallyWiredOperatorChain : public omnistream::OperatorChainV2 {
public:
    ManuallyWiredOperatorChain() : OperatorChainV2()
    {
        mainOperatorOutput = nullptr;
        mainOperatorWrapper = nullptr;
        tailOperatorWrapper = nullptr;
        operatorEventDispatcher = nullptr;
    }

    void install(
        OneInputStreamOperator* head,
        OneInputStreamOperator* middle,
        OneInputStreamOperator* tail,
        WatermarkGaugeExposingOutput* mainOutput)
    {
        mainOperatorOutput = mainOutput;

        auto* tailWrapper = new StreamOperatorWrapper(tail, false);
        auto* middleWrapper = new StreamOperatorWrapper(middle, false);
        auto* headWrapper = new StreamOperatorWrapper(head, true);

        headWrapper->setNext(middleWrapper);
        middleWrapper->setPrevious(headWrapper);
        middleWrapper->setNext(tailWrapper);
        tailWrapper->setPrevious(middleWrapper);

        mainOperatorWrapper = headWrapper;
        tailOperatorWrapper = tailWrapper;
        mainOperatorWrapper->setAsHead();
    }
};

omnistream::OperatorPOD makeOperatorPod(
    const std::string& name, const std::string& id, const std::string& operatorId, int vertexId, bool keyed)
{
    omnistream::OperatorPOD pod;
    pod.setName(name);
    pod.setId(id);
    pod.setOperatorId(operatorId);
    pod.setVertexID(vertexId);
    pod.setJobType(omnistream::Type_o::STREAM);
    pod.setTaskType(omnistream::Type_o::STREAM);
    pod.setVOperatorType(omnistream::Type_o::STREAM);
    if (keyed) {
        pod.setDescription(
            R"({"stateKeyTypes":{"serializerName":"org.apache.flink.api.common.typeutils.base.LongSerializer"}})");
    } else {
        pod.setDescription("{}");
    }
    return pod;
}

omnistream::StreamConfigPOD makeStreamConfig(const omnistream::OperatorPOD& pod)
{
    omnistream::StreamConfigPOD config;
    config.setOperatorDescription(pod);
    config.setNumberOfNetworkInputs(1);
    return config;
}

omnistream::TaskInformationPOD makeOperatorChainTaskInformation()
{
    omnistream::TaskInformationPOD taskInfo;
    taskInfo.setTaskName("dt-map-keyed-reduce-map-heap-state");
    taskInfo.setNumberOfSubtasks(1);
    taskInfo.setMaxNumberOfSubtasks(kNumberOfKeyGroups);
    taskInfo.setIndexOfSubtask(0);
    // OmniStream currently creates HeapKeyedStateBackend through the HashMapStateBackend branch.
    taskInfo.setStateBackend(kHeapBackendTaskStateBackend);
    taskInfo.SetTaskType(omnistream::Type_o::STREAM);

    auto head = makeStreamConfig(makeOperatorPod("sourceMap", "dt.source.map", kSourceMapOperatorId, 1, false));
    auto reduce = makeStreamConfig(makeOperatorPod("keyedReduce", "dt.keyed.reduce", kReduceOperatorId, 2, true));
    auto tail = makeStreamConfig(makeOperatorPod("sinkMap", "dt.sink.map", kSinkMapOperatorId, 3, false));

    taskInfo.setStreamConfigPOD(head);
    taskInfo.setChainedConfig({reduce, tail});
    return taskInfo;
}

std::shared_ptr<omnistream::TaskStateManager> makeTaskStateManager(
    const std::shared_ptr<omnistream::TaskStateManagerBridge>& stateManagerBridge,
    const std::shared_ptr<omnistream::OmniTaskBridge>& omniTaskBridge,
    const std::shared_ptr<TaskStateSnapshot>& restoreSnapshot = nullptr,
    long restoreCheckpointId = -1)
{
    auto jobId = omnistream::JobIDPOD(1, 2);
    auto jobVertexId = omnistream::JobVertexID(3, 4);
    auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
    auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
    auto* responder = new omnistream::NoOpCheckpoingResponder();
    std::shared_ptr<omnistream::JobManagerTaskRestore> restore;
    if (restoreSnapshot != nullptr) {
        restore = std::make_shared<omnistream::JobManagerTaskRestore>(restoreCheckpointId, restoreSnapshot);
    }
    return std::make_shared<omnistream::TaskStateManager>(
        jobId, attemptId, stateStore, responder, stateManagerBridge, omniTaskBridge, restore);
}

struct OperatorChainReduceHarness {
    explicit OperatorChainReduceHarness(
        std::shared_ptr<InMemoryOmniStateBridge> bridge,
        std::shared_ptr<TaskStateSnapshot> restoreSnapshot = nullptr,
        long restoreCheckpointId = -1)
        : omniBridge(std::move(bridge)),
          stateManagerBridge(std::make_shared<NoLocalStateTaskStateManagerBridge>()),
          taskInfo(makeOperatorChainTaskInformation()),
          env(std::make_shared<omnistream::RuntimeEnvironmentV2>())
    {
        env->setTaskConfiguration(taskInfo);
        env->SetTaskStateManager(
            makeTaskStateManager(stateManagerBridge, omniBridge, restoreSnapshot, restoreCheckpointId));

        auto sink = std::make_unique<CollectingSinkOutput>();
        sinkOutput = sink.get();

        sinkMap = new MapOperatorForDt(sink.get(), "sinkMap(+1000)", 1, 1000);
        auto reduceOutput = std::make_unique<KeyContextAwareChainingOutput>(sinkMap);

        keyedReduce = new HeapKeyedReduceOperatorForDt(reduceOutput.get());
        auto sourceOutput = std::make_unique<KeyContextAwareChainingOutput>(keyedReduce);
        auto* mainOutput = sourceOutput.get();

        sourceMap = new MapOperatorForDt(sourceOutput.get(), "sourceMap(*2)", 2, 0);

        static_cast<OneInputStreamOperator*>(sourceMap)->SetOperatorID(kSourceMapOperatorId);
        static_cast<OneInputStreamOperator*>(keyedReduce)->SetOperatorID(kReduceOperatorId);
        static_cast<OneInputStreamOperator*>(sinkMap)->SetOperatorID(kSinkMapOperatorId);

        outputs.emplace_back(std::move(sink));
        outputs.emplace_back(std::move(reduceOutput));
        outputs.emplace_back(std::move(sourceOutput));

        chain = std::make_unique<ManuallyWiredOperatorChain>();
        chain->install(sourceMap, keyedReduce, sinkMap, mainOutput);

        StreamTaskStateInitializerImpl initializer(env.get());
        chain->initializeStateAndOpenOperators(&initializer, taskInfo);
        if (sourceMap->wasInitializedWithKeySerializer() || sinkMap->wasInitializedWithKeySerializer()) {
            throw std::runtime_error("stateless map operator was initialized with a key serializer");
        }
        if (!keyedReduce->wasInitializedWithKeySerializer()) {
            throw std::runtime_error("keyed reduce was not initialized with a key serializer");
        }
        if (!sourceMap->wasOpened() || !keyedReduce->wasOpened() || !sinkMap->wasOpened()) {
            throw std::runtime_error("operator chain was not fully opened");
        }
        if (!keyedReduce->usesHeapKeyedStateBackend()) {
            throw std::runtime_error("keyed reduce did not initialize HeapKeyedStateBackend");
        }

        input = std::make_unique<omnistream::datastream::StreamTaskNetworkOutput>(chain->getMainOperator(), 0);
    }

    void process(const InputRecord& record)
    {
        ChainRecord chainRecord{record.key, record.value};
        StreamRecord streamRecord(&chainRecord);
        input->emitRecord(&streamRecord);
    }

    std::vector<SinkRecord> processAll(const std::vector<InputRecord>& records)
    {
        const size_t begin = sinkOutput->records().size();
        for (const auto& record : records) {
            process(record);
        }
        const auto& allRecords = sinkOutput->records();
        return std::vector<SinkRecord>(allRecords.begin() + static_cast<long>(begin), allRecords.end());
    }

    std::vector<long> keyedStateFor(const std::vector<int>& keys)
    {
        std::vector<long> values;
        values.reserve(keys.size());
        for (int key : keys) {
            values.push_back(keyedReduce->stateValueForKey(key));
        }
        return values;
    }

    std::shared_ptr<InMemoryOmniStateBridge> omniBridge;
    std::shared_ptr<omnistream::TaskStateManagerBridge> stateManagerBridge;
    omnistream::TaskInformationPOD taskInfo;
    std::shared_ptr<omnistream::RuntimeEnvironmentV2> env;
    std::vector<std::unique_ptr<WatermarkGaugeExposingOutput>> outputs;
    std::unique_ptr<ManuallyWiredOperatorChain> chain;
    std::unique_ptr<omnistream::datastream::StreamTaskNetworkOutput> input;
    CollectingSinkOutput* sinkOutput = nullptr;
    MapOperatorForDt* sourceMap = nullptr;
    HeapKeyedReduceOperatorForDt* keyedReduce = nullptr;
    MapOperatorForDt* sinkMap = nullptr;
};

OperatorID operatorIdFromHex(const std::string& operatorId)
{
    return TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorId);
}

// This chain does not register timers, so raw keyed timer state would only send restore down
// the legacy timer path and mask the managed keyed state consistency signal.
std::shared_ptr<OperatorSubtaskState> withoutRawKeyedStateForNoTimerDt(
    const std::shared_ptr<OperatorSubtaskState>& state)
{
    if (state == nullptr || state->getRawKeyedState().IsEmpty()) {
        return state;
    }

    auto managedOperatorState = state->getManagedOperatorState();
    auto rawOperatorState = state->getRawOperatorState();
    auto managedKeyedState = state->getManagedKeyedState();
    StateObjectCollection<KeyedStateHandle> rawKeyedState;
    auto inputChannelState = state->getInputChannelState();
    auto resultSubpartitionState = state->getResultSubpartitionState();
    return std::make_shared<OperatorSubtaskState>(
        managedOperatorState,
        rawOperatorState,
        managedKeyedState,
        rawKeyedState,
        inputChannelState,
        resultSubpartitionState);
}

std::shared_ptr<TaskStateSnapshot> snapshotOperatorChainToTaskState(
    OperatorChainReduceHarness& harness, CheckpointOptions* checkpointOptions, long checkpointId)
{
    std::unordered_map<OperatorID, OperatorSnapshotFutures*> snapshots;
    CheckpointMetaData checkpointMetaData(checkpointId, 0);
    auto isRunning = std::make_shared<omnistream::LambdaSupplier<bool>>([]() { return std::make_shared<bool>(true); });
    auto channelStateResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    harness.chain->PrepareSnapshotPreBarrier(checkpointId);
    harness.chain->SnapshotState(
        &snapshots, checkpointMetaData, checkpointOptions, isRunning, channelStateResult, nullptr, harness.omniBridge);
    if (snapshots.size() != 3) {
        throw std::runtime_error("operator chain snapshot count mismatch: " + std::to_string(snapshots.size()));
    }

    auto taskSnapshot = std::make_shared<TaskStateSnapshot>();
    for (auto& entry : snapshots) {
        OperatorSnapshotFinalizer finalizer(entry.second);
        taskSnapshot->PutSubtaskStateByOperatorID(
            entry.first, withoutRawKeyedStateForNoTimerDt(finalizer.getJobManagerOwnedState()));
        delete entry.second;
    }

    // The consistency contract belongs to keyed reduce state ownership and restore, not to how
    // stateless chained maps materialize empty OperatorSubtaskState on different environments.
    auto reduceState = taskSnapshot->GetSubtaskStateByOperatorID(operatorIdFromHex(kReduceOperatorId));
    if (reduceState == nullptr || reduceState->getManagedKeyedState().IsEmpty()) {
        throw std::runtime_error("operator chain snapshot did not contain keyed reduce managed state");
    }
    if (!reduceState->getRawKeyedState().IsEmpty()) {
        throw std::runtime_error("operator chain no-timer DT unexpectedly retained raw keyed timer state");
    }
    auto handles = reduceState->getManagedKeyedState().AsList();
    if (handles.empty()) {
        throw std::runtime_error("operator chain keyed reduce managed state handle list is empty");
    }
    if (std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(handles.front()) == nullptr) {
        throw std::runtime_error("operator chain snapshot did not produce KeyGroupsSavepointStateHandle");
    }

    return taskSnapshot;
}

void assertOperatorChainRestoreKeepsHeapKeyedReduceStateConsistent(CheckpointOptions* checkpointOptions)
{
    const std::vector<InputRecord> beforeSnapshot = {{1, 3}, {2, 4}, {1, 5}, {9, 7}};
    const std::vector<InputRecord> afterRestore = {{2, 6}, {1, 8}, {3, 10}, {9, 1}};
    const std::vector<int> observedKeys = {1, 2, 3, 9};

    OperatorChainReduceHarness baseline(std::make_shared<InMemoryOmniStateBridge>());
    baseline.processAll(beforeSnapshot);
    const auto expectedTailSink = baseline.processAll(afterRestore);
    const auto expectedFinalState = baseline.keyedStateFor(observedKeys);

    auto bridge = std::make_shared<InMemoryOmniStateBridge>();
    OperatorChainReduceHarness beforeFailure(bridge);
    beforeFailure.processAll(beforeSnapshot);
    const auto stateBeforeSnapshot = beforeFailure.keyedStateFor(observedKeys);

    const long checkpointId = 42;
    auto taskSnapshot = snapshotOperatorChainToTaskState(beforeFailure, checkpointOptions, checkpointId);

    OperatorChainReduceHarness restored(bridge, taskSnapshot, checkpointId);
    const auto stateAfterRestore = restored.keyedStateFor(observedKeys);
    const auto actualTailSink = restored.processAll(afterRestore);
    const auto actualFinalState = restored.keyedStateFor(observedKeys);

    EXPECT_EQ(stateAfterRestore, stateBeforeSnapshot);
    EXPECT_EQ(actualTailSink, expectedTailSink);
    EXPECT_EQ(actualFinalState, expectedFinalState);
}

} // namespace

// Backend-level guard for the heap keyed reduce state checkpoint path.
// Verifies snapshot -> HeapKeyedStateBackendBuilder restore preserves per-key state,
// then compares post-restore sink output and final reduce state against a no-failure baseline.
TEST(HeapStateBackendDtTest, BackendCheckpointRestoreKeepsKeyedReduceStateConsistent)
{
    std::unique_ptr<CheckpointOptions> options(
        CheckpointOptions::AlignedNoTimeout(
            *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault()));
    assertBackendSnapshotRestoreKeepsReduceStateConsistent(options.get());
}

// Backend-level guard for the canonical savepoint path.
// Uses the same map -> keyed reduce -> map data pattern as checkpoint coverage,
// but forces SavepointType::CANONICAL to exercise savepoint metadata/data restore.
TEST(HeapStateBackendDtTest, BackendSavepointRestoreKeepsKeyedReduceStateConsistent)
{
    auto savepointType = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    std::unique_ptr<CheckpointOptions> options(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));
    assertBackendSnapshotRestoreKeepsReduceStateConsistent(options.get());
}

// OperatorChain-level checkpoint guard for StreamTask-style execution without JNI/connectors.
// Drives StreamTaskNetworkOutput -> map -> HeapKeyed reduce -> map -> collecting sink,
// snapshots through OperatorChainV2, restores via JobManagerTaskRestore, and checks state/output consistency.
TEST(HeapStateBackendDtTest, OperatorChainCheckpointRestoreKeepsHeapKeyedReduceStateConsistent)
{
    std::unique_ptr<CheckpointOptions> options(
        CheckpointOptions::AlignedNoTimeout(
            *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault()));
    assertOperatorChainRestoreKeepsHeapKeyedReduceStateConsistent(options.get());
}

// OperatorChain-level canonical savepoint guard.
// Ensures the keyed reduce state is snapshotted and savepoint restore produces the same
// immediate reduce state, downstream records, and final reduce state as the baseline.
TEST(HeapStateBackendDtTest, OperatorChainSavepointRestoreKeepsHeapKeyedReduceStateConsistent)
{
    auto savepointType = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    std::unique_ptr<CheckpointOptions> options(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));
    assertOperatorChainRestoreKeepsHeapKeyedReduceStateConsistent(options.get());
}
