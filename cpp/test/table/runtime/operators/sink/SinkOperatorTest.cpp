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

#include "table/runtime/operators/sink/SinkOperator.h"
#include "runtime/operators/sink/TimeStampInserterSinkOperator.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "runtime/io/network/api/writer/RecordWriter.h"
#include "runtime/io/network/api/writer/V2/RecordWriterV2.h"
#include "runtime/io/network/api/writer/V2/SimpleSelectorRecordWriterV2.h"
#include "streaming/runtime/io/RecordWriterOutput.h"
#include "streaming/runtime/io/RecordWriterOutputV2.h"
#include "streaming/runtime/partitioner/V2/ChannelSelectorV2.h"
#include "test/table/runtime/operators/DummyStreamPartitioner.h"
#include <gtest/gtest.h>
#include <vector/vector_helper.h>
#include <test/util/test_util.h>

using namespace omniruntime::vec;
TEST(SinkOperatorTest, InitTest)
{
    std::string sinkDescription = R"({"outputfile":"/tmp/flink_output.txt"})";
    auto obj = nlohmann::json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    op.open();

    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    StreamRecord* record = new StreamRecord(rowData);
    op.processElement(record);
};

TEST(SinkOperatorTest, DISABLED_BatchOutputTest)
{
    std::string sinkDescription = R"({"outputfile":"/tmp/flink_output.txt"})";
    auto obj = nlohmann::json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    int vectorSize = 5;

    omnistream::VectorBatch vb(vectorSize);
    Vector<int>* vec0 = new Vector<int>(vectorSize);
    for (int i = 0; i < vectorSize; i++) {
        vec0->SetValue(i, i);
    }
    vb.Append(vec0);

    auto* vec1 = new Vector<LargeStringContainer<std::string_view>>(vectorSize, 2048);

    std::string valuePrefix;
    valuePrefix = "hello_world__";

    for (int32_t i = 0; i < vectorSize; i++) {
        std::string value = valuePrefix + std::to_string(i);
        std::string_view input(value.data(), value.size());
        vec1->SetValue(i, input);
    }

    vb.Append(vec1);
    op.processBatch(new StreamRecord(&vb));
}

omnistream::VectorBatch* createVectorBatch()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> bids = {9986, 9067, 7973, 9442, 9285};
    std::vector<int64_t> timeStamps = {5, 13, 18, 22, 27};
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(bids.size(), bids.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(bids.size(), timeStamps.data()));
    for (int i = 0; i < bids.size(); i++) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    return vbatch;
}

class MockResultPartitionWriter : public omnistream::ResultPartitionWriter {
public:
    MockResultPartitionWriter() = default;

    void setup() override
    {
    }

    ResultPartitionIDPOD getPartitionId() override
    {
        return ResultPartitionIDPOD();
    }

    int getNumberOfSubpartitions() override
    {
        return 0;
    }

    int getNumTargetKeyGroups() override
    {
        return 0;
    }

    void emitRecord(void* record, int targetSubpartition) override
    {
    }

    void broadcastRecord(void* record) override
    {
    }

    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent) override
    {
    }

    void NotifyEndOfData(StopMode mode) override
    {
    }

    shared_ptr<CompletableFuture> getAllDataProcessedFuture() override
    {
        return std::shared_ptr<CompletableFuture>();
    }

    shared_ptr<ResultSubpartitionView> createSubpartitionView(
        int index, BufferAvailabilityListener* availabilityListener) override
    {
        return std::shared_ptr<ResultSubpartitionView>();
    }

    void flushAll() override
    {
    }

    void flush(int subpartitionIndex) override
    {
    }

    void fail(std::optional<std::exception_ptr> throwable) override
    {
    }

    void finish() override
    {
    }

    bool isFinished() override
    {
        return false;
    }

    void release(std::optional<std::exception_ptr> cause) override
    {
    }

    bool isReleased() override
    {
        return false;
    }

    void cancel() override
    {
    }

    void close() override
    {
    }

    string toString() const override
    {
        return "MockResultPartitionWriter";
    }

    shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        return std::shared_ptr<CompletableFuture>();
    }

    bool isAvailable() override
    {
        return AvailabilityProvider::isAvailable();
    }

    string toString() override
    {
        return AvailabilityProvider::toString();
    }
};

class MockChannelSelectorV2 : public omnistream::ChannelSelectorV2<StreamRecord> {
public:
    MockChannelSelectorV2() = default;

    void setup(int numberOfChannels) override
    {
    }

    unordered_map<int, StreamRecord*> selectChannel(StreamRecord* record) override
    {
        return unordered_map<int, StreamRecord*>();
    }

    bool isBroadcast() const override
    {
        return false;
    }
};

TEST(TimeStampSinkOperatorTest, InitTest)
{
    std::string desc = R"DELIM({"rowtimeFieldIndex": 1})DELIM";
    auto obj = nlohmann::json::parse(desc);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner();
    auto targetPartitionWriter = std::make_shared<MockResultPartitionWriter>();
    auto* recordWriter = new omnistream::SimpleSelectorRecordWriterV2(
        targetPartitionWriter, new MockChannelSelectorV2(), nullptr, 1000, "test", 1);
    auto* recordWriteOutput = new omnistream::RecordWriterOutputV2(recordWriter, binaryRowDataSerializer);
    auto op = TimeStampInserterSinkOperator(obj, recordWriteOutput, obj);
    op.open();
    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    StreamRecord* record = new StreamRecord(rowData);
    op.processElement(record);
    omnistream::VectorBatch* batch = createVectorBatch();
    StreamRecord* record2 = new StreamRecord(batch);
    op.processBatch(record2);
}
