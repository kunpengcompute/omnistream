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

#include <iostream>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>

#include <librdkafka/rdkafkacpp.h>

// Pre-include KafkaSourceReader.h's direct dependencies so that their
// include guards are set before the `#define private public` below.
// This ensures only KafkaSourceReader.h is affected by the macro.
#include "connector/kafka/source/metrics/KafkaSourceReaderMetrics.h"
#include "connector/kafka/source/reader/SingleThreadMultiplexSourceReaderBase.h"

#define private public
#include "connector/kafka/source/reader/KafkaSourceReader.h"
#undef private

#include "connector/kafka/source/reader/RecordEmitter.h"
#include "connector/kafka/source/reader/fetcher/KafkaSourceFetcherManager.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceReaderContext.h"

namespace {
// Minimal RecordEmitter mock that does nothing on emit.
class MockRecordEmitter : public RecordEmitter<RdKafka::Message, KafkaPartitionSplitState> {
public:
    void emitRecord(RdKafka::Message*, SourceOutput*, KafkaPartitionSplitState*) override
    {
    }
    void emitBatchRecord(const std::vector<RdKafka::Message*>&, SourceOutput*, KafkaPartitionSplitState*) override
    {
    }
};

class QueueNotifyingSplitReader : public SplitReader<RdKafka::Message, KafkaPartitionSplit> {
public:
    QueueNotifyingSplitReader(FutureCompletingBlockingQueue<RdKafka::Message>* queue, bool& closed)
        : queue(queue),
          closed(closed)
    {
    }

    RecordsWithSplitIds<RdKafka::Message>* fetch() override
    {
        return nullptr;
    }

    void handleSplitsChanges(const std::vector<KafkaPartitionSplit*>&) override
    {
    }

    void wakeUp() override
    {
    }

    void close() override
    {
        // Fetcher shutdown must finish while its queue is still alive.
        // Use the queue API: after enqueue, its future is the AVAILABLE marker,
        // which must not be assumed to be a dereferenceable future here.
        queue->notifyAvailable();
        closed = true;
    }

private:
    FutureCompletingBlockingQueue<RdKafka::Message>* queue;
    bool& closed;
};

class TrackedRecords : public RecordsWithSplitIds<RdKafka::Message> {
public:
    explicit TrackedRecords(int& liveBatches) : liveBatches(liveBatches)
    {
        ++liveBatches;
    }
    ~TrackedRecords() override
    {
        --liveBatches;
    }
    std::string nextSplit() override
    {
        return "";
    }
    const RdKafka::Message* nextRecordFromSplit() override
    {
        return nullptr;
    }
    const std::vector<RdKafka::Message*>& getRecordsFromSplit() override
    {
        return records;
    }
    size_t getSplitStoppingOffset() override
    {
        return 0;
    }
    std::set<std::string>& finishedSplits() override
    {
        return finished;
    }

private:
    int& liveBatches;
    std::vector<RdKafka::Message*> records;
    std::set<std::string> finished;
};

class BatchTrackingSplitReader : public QueueNotifyingSplitReader {
public:
    BatchTrackingSplitReader(FutureCompletingBlockingQueue<RdKafka::Message>* queue, bool& closed, int& liveBatches)
        : QueueNotifyingSplitReader(queue, closed),
          queue(queue),
          liveBatches(liveBatches)
    {
    }

    ~BatchTrackingSplitReader() override
    {
        // A real consumer can block here while messages retain broker references.
        EXPECT_EQ(liveBatches, 0);
    }

    RecordsWithSplitIds<RdKafka::Message>* fetch() override
    {
        // Make a full-queue put return false, leaving this batch in FetchTask.
        queue->wakeUpPuttingThread(0);
        return new TrackedRecords(liveBatches);
    }

private:
    FutureCompletingBlockingQueue<RdKafka::Message>* queue;
    int& liveBatches;
};
} // namespace

class KafkaSourceReaderLifecycleTest : public ::testing::TestWithParam<bool> {};

TEST_P(KafkaSourceReaderLifecycleTest, JoinsFetcherBeforeDestroyingQueue)
{
    bool splitReaderClosed = false;
    auto* queue = new FutureCompletingBlockingQueue<RdKafka::Message>(0);
    auto availabilityFuture = queue->getAvailabilityFuture();
    std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> supplier = [&]() {
        return new QueueNotifyingSplitReader(queue, splitReaderClosed);
    };
    auto* manager = new KafkaSourceFetcherManager(queue, supplier);
    auto reader = std::make_unique<KafkaSourceReader>(
        queue,
        manager,
        new MockRecordEmitter(),
        std::unordered_map<std::string, std::string>{},
        new SourceReaderContext(0),
        false);
    manager->startFetcher(manager->createSplitFetcher());

    if (GetParam()) {
        reader->close();
    }
    reader.reset();

    // Joining the worker also synchronizes its write to splitReaderClosed.
    EXPECT_TRUE(splitReaderClosed);
    EXPECT_TRUE(availabilityFuture->isDone());
}

INSTANTIATE_TEST_SUITE_P(WithAndWithoutExplicitClose, KafkaSourceReaderLifecycleTest, ::testing::Bool());

TEST_P(KafkaSourceReaderLifecycleTest, ReleasesQueuedAndInterruptedBatchesBeforeConsumer)
{
    bool splitReaderClosed = false;
    int liveBatches = 0;
    auto* queue = new FutureCompletingBlockingQueue<RdKafka::Message>(0, 1);
    BatchTrackingSplitReader* splitReader = nullptr;
    std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> supplier = [&]() {
        splitReader = new BatchTrackingSplitReader(queue, splitReaderClosed, liveBatches);
        return splitReader;
    };
    auto* manager = new KafkaSourceFetcherManager(queue, supplier);
    auto reader = std::make_unique<KafkaSourceReader>(
        queue,
        manager,
        new MockRecordEmitter(),
        std::unordered_map<std::string, std::string>{},
        new SourceReaderContext(0),
        false);
    auto* fetcher = manager->createSplitFetcher();
    ASSERT_TRUE(queue->put(0, new TrackedRecords(liveBatches)));
    {
        FetchTask<RdKafka::Message, KafkaPartitionSplit> task(
            splitReader, queue, [](const std::set<std::string>&) {}, 0);
        task.fetch();
        EXPECT_EQ(liveBatches, 2);
    }
    EXPECT_EQ(liveBatches, 1);
    // Exercise both an unstarted fetcher and one retired by its shutdown hook.
    if (GetParam()) {
        fetcher->shutdown();
        manager->startFetcher(fetcher);
    }
    reader.reset();
    EXPECT_EQ(liveBatches, 0);
}

class KafkaSourceReaderConfigTest : public ::testing::Test {
protected:
    // Create a KafkaSourceReader with the given props.
    // The reader takes ownership of all heap-allocated dependencies.
    std::unique_ptr<KafkaSourceReader> createReader(
        const std::unordered_map<std::string, std::string>& props, bool isBatch = false)
    {
        auto* elementsQueue = new FutureCompletingBlockingQueue<RdKafka::Message>(0);
        std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> supplier =
            []() -> SplitReader<RdKafka::Message, KafkaPartitionSplit>* { return nullptr; };
        auto* fetcherManager = new KafkaSourceFetcherManager(elementsQueue, supplier);
        auto* recordEmitter = new MockRecordEmitter();
        auto* context = new SourceReaderContext(0);
        return std::unique_ptr<KafkaSourceReader>(
            new KafkaSourceReader(elementsQueue, fetcherManager, recordEmitter, props, context, isBatch));
    }
};

// "false" -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsFalse)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "false";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// "true" -> enabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsEnabledWhenConfigIsTrue)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "true";
    auto reader = createReader(props);
    EXPECT_TRUE(reader->commitOffsetsOnCheckpoint_);
}

// Missing property -> disabled (default)
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledByDefault)
{
    std::unordered_map<std::string, std::string> props;
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Non-"true" value -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsNotTrue)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "yes";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Empty string value -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsEmpty)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Case sensitivity: "True" is not "true" -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsCaseSensitive)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "True";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Behavioral: when disabled, snapshotState returns early and
// notifyCheckpointComplete does not touch offsetsToCommit_.
TEST_F(KafkaSourceReaderConfigTest, SnapshotAndNotifyWhenDisabled)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "false";
    auto reader = createReader(props);
    auto splits = reader->snapshotState(1);
    EXPECT_TRUE(splits.empty());
    // No crash: notifyCheckpointComplete returns before accessing offsetsToCommit_.
    reader->notifyCheckpointComplete(1);
    // offsetsToCommit_ should remain empty since snapshotState returned early.
    EXPECT_TRUE(reader->offsetsToCommit_.empty());
}

// Behavioral: when enabled, snapshotState populates offsetsToCommit_ and
// notifyCheckpointComplete drains it.
TEST_F(KafkaSourceReaderConfigTest, SnapshotAndNotifyWhenEnabled)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "true";
    auto reader = createReader(props);
    auto splits = reader->snapshotState(1);
    EXPECT_TRUE(splits.empty());
    // snapshotState should have recorded an (empty) offsets entry for checkpoint 1.
    EXPECT_FALSE(reader->offsetsToCommit_.empty());
    EXPECT_EQ(reader->offsetsToCommit_.count(1), 1u);
    // notifyCheckpointComplete will call commitOffsets with an empty map,
    // which returns early (no crash).
    reader->notifyCheckpointComplete(1);
}
