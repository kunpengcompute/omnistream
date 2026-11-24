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

#include <condition_variable>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>

#include "streaming/runtime/partitioner/V2/ForwardPartitionerV2.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "event/EndOfPartitionEvent.h"
#include "runtime/taskexecutor/TaskManagerServices.h"
#include "runtime/io/network/api/writer/V2/RecordWriterBuilderV2.h"
#include "runtime/io/network/api/writer/V2/RecordWriterV2.h"
#include "partition/consumer/InputGate.h"
#include "partition/consumer/SingleInputGate.h"
#include "runtime/executiongraph/descriptor/ExecutionAttemptIDPOD.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "runtime/partition/ResultPartitionWriter.h"
#include "runtime/shuffle/ShuffleEnvironment.h"
#include "runtime/shuffle/ShuffleIOOwnerContextPOD.h"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "runtime/taskmanager/OmniTask.h"
#include "streaming/runtime/tasks/omni/OmniStreamTask.h"
#include "test/functionaltest/e2e/FrameworkConfig.h"

/**
 * Create 3 threads to simulate the shuffle environment
 * 1. Upstream Task
 * - Contains RecordWriter that writes data to ResultPartition
 * 2. Downstream Task
 * - Contains InputGate and InputChannel that reads data from ResultPartition
 * 3. OutputFlusher
 * - In FlinkSQL, OutputFlusher is a thread that flushes data to ResultPartition spawned by RecordWriter
 */
class ShuffleEnvironmentTest : public FrameworkTestConfig {
public:
    void SetUp() override
    {
        FrameworkTestConfig::SetUp();

        // TaskManagerServices
        std::ifstream taskMCSConfFile("./input/taskMCSConf.json");
        if (!taskMCSConfFile.is_open()) {
            throw std::runtime_error("Failed to open taskMCSConf.json");
        }
        try {
            taskMCSConfJson_ = nlohmann::json::parse(taskMCSConfFile);
        } catch (const nlohmann::json::parse_error& e) {
            throw std::runtime_error("JSON parse error: " + std::string(e.what()));
        }
        // taskMCSConfJson_ = nlohmann::json::parse(taskMCSConfFile);

        // Source Task
        std::ifstream jobFile("./input/job.json");
        nlohmann::json jobJson = nlohmann::json::parse(jobFile); // same for both source and sink

        std::ifstream sourceTaskFile("./input/sourceTask.json");
        nlohmann::json sourceTaskJson = nlohmann::json::parse(sourceTaskFile);
        std::cout << sourceTaskJson << std::endl;
        std::ifstream sourceTddFile("./input/sourceTdd.json");
        nlohmann::json sourceTddJson = nlohmann::json::parse(sourceTddFile);

        // Sink Task
        std::ifstream sinkTaskFile("./input/sinkTask.json");
        nlohmann::json sinkTaskJson = nlohmann::json::parse(sinkTaskFile);
        std::cout << sinkTaskJson << std::endl;

        std::ifstream sinkTddFile("./input/sinkTdd.json");
        nlohmann::json sinkTddJson = nlohmann::json::parse(sinkTddFile);

        jobInfo_ = jobJson;
        srcTaskInfo_ = sourceTaskJson;
        srcTddInfo_ = sourceTddJson;
        sinkTaskInfo_ = sinkTaskJson;
        sinkTddInfo_ = sinkTddJson;

        // TaskManagerServices
        taskManagerServices_ = std::shared_ptr<omnistream::TaskManagerServices>(
            omnistream::TaskManagerServices::fromConfiguration(taskMCSConfJson_));

        // ShuffleEnvironment
        shuffleEnv_ = taskManagerServices_->getShuffleEnvironment();

        // Upstream Writer and Downstream InputGate
        createUpstreamWriterAndDownstreamInputGate();

        // Dummy VectorBatch and Record
        std::vector<omniruntime::type::DataTypeId> types = {omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_LONG};
        dummyVectorBatch_ = omnistream::createVectorBatch(types, 3, true);
        dummyRecord_ = std::make_shared<StreamRecord>(dummyVectorBatch_);
    }

    void createUpstreamWriterAndDownstreamInputGate()
    {
        auto shuffleIOOwnerContext = shuffleEnv_->createShuffleIOOwnerContext(
            "testShuffleIOOwnerContext", omnistream::ExecutionAttemptIDPOD(), nullptr);

        // Upstream ResultPartitionWriters
        auto resultPartitionWriters =
            shuffleEnv_->createResultPartitionWriters(shuffleIOOwnerContext, srcTddInfo_.getProducedPartitions(), 1);
        upstreamWriter_ = resultPartitionWriters[0]; // This query only have 1 resultPartition

        // Downstream InputGates
        auto inputGates =
            shuffleEnv_->createInputGates(shuffleIOOwnerContext, nullptr, sinkTddInfo_.getInputGates(), 1);
        downstreamInputGate_ = inputGates[0]; // This query only have 1 inputGate

        // RecordWriter
        auto partitionerName = getPartitionerName(srcTaskInfo_);
        auto partitioner = getPartitionerFromPartitionerName(partitionerName);

        auto bufferTimeout = srcTaskInfo_.getStreamConfigPOD().getOutEdgesInOrder()[0].getBufferTimeout();
        recordWriter_ = std::shared_ptr<omnistream::RecordWriterV2>(omnistream::RecordWriterBuilderV2()
                                                                        .withTaskName("testRecordWriter")
                                                                        .withChannelSelector(partitioner.get())
                                                                        .withWriter(upstreamWriter_)
                                                                        .withTimeout(bufferTimeout)
                                                                        .build());

        recordWriter_->postConstruct(); // start the output flusher thread

        // setupPartitionsAndGates
        upstreamWriter_->setup();      // partitionManager->registerResultPartition
        downstreamInputGate_->setup(); // setBufferPool and setupChannels

        // requestSubpartition, connect upstream and downstream
        downstreamInputGate_->RequestPartitions(); // partitionManager->createSubpartitionView

        targetSubpartition_ = srcTddInfo_.getProducedPartitions()[0].getPartitionId().getPartitionNum();
    }

    void checkVectorBatchMatch(omniruntime::vec::VectorBatch* vecBatch, omniruntime::vec::VectorBatch* expectedVecBatch)
    {
        EXPECT_EQ(vecBatch->GetRowCount(), expectedVecBatch->GetRowCount());
        EXPECT_EQ(vecBatch->GetVectorCount(), expectedVecBatch->GetVectorCount());
        for (int i = 0; i < vecBatch->GetVectorCount(); i++) {
            for (int j = 0; j < vecBatch->GetRowCount(); j++) {
                EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(i))->GetValue(j), j);
            }
        }
    }

    void emitRecordsThread()
    {
        for (int i = 0; i < 10; i++) {
            upstreamWriter_->emitRecord(dummyRecord_.get(), targetSubpartition_);
        }
        upstreamWriter_->finish(); // send END_OF_PARTITION_EVENT

        recordWriter_->close();
        upstreamWriter_->close();
    }

    int pollRecordsThread()
    {
        int recordCount = 0;
        while (true) {
            auto bufferOrEvent = downstreamInputGate_->PollNext();
            if (!bufferOrEvent) {
                continue;
            }

            if (bufferOrEvent->isBuffer()) {
                auto buff = reinterpret_cast<ObjectBuffer*>(bufferOrEvent->getBuffer());

                auto size = buff->GetSize();
                auto objSegment = buff->GetObjectSegment();
                auto offset = buff->GetOffset();

                for (int64_t index = offset; index < size; index++) {
                    StreamRecord* object = static_cast<StreamRecord*>(objSegment->getObject(index));
                    assert(object->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
                    auto vecBatch = static_cast<omniruntime::vec::VectorBatch*>(object->getValue());
                    // LOG("!!!! pollRecordsThread start")
                    // omniruntime::vec::VectorHelper::PrintVecBatch(reinterpret_cast<omniruntime::vec::VectorBatch*>(object->getValue()));
                    // LOG("!!!! pollRecordsThread end")
                    /**
                     * Expected Output from `omniruntime::vec::VectorHelper::PrintVecBatch(vecBatch);`
                     * 0, 0
                     * 1, 1
                     * 2, 2
                     */
                    checkVectorBatchMatch(vecBatch, dummyVectorBatch_);
                    recordCount++;
                }
            } else {
                auto event = bufferOrEvent->getEvent();
                if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
                    break;
                }
            }
            delete bufferOrEvent;
        }
        downstreamInputGate_->close();
        return recordCount;
    }

protected:
    std::shared_ptr<omnistream::TaskManagerServices> taskManagerServices_;
    std::shared_ptr<omnistream::ShuffleEnvironment> shuffleEnv_;
    std::shared_ptr<omnistream::RecordWriterV2> recordWriter_;

    std::shared_ptr<omnistream::SingleInputGate> downstreamInputGate_;
    std::shared_ptr<omnistream::ResultPartitionWriter> upstreamWriter_;

    omniruntime::vec::VectorBatch* dummyVectorBatch_;
    std::shared_ptr<StreamRecord> dummyRecord_;
    int targetSubpartition_;

    std::mutex mtx_;
    std::condition_variable cv_;
};

TEST_F(ShuffleEnvironmentTest, DISABLED_UpstreamSendsRecordsAndDownstreamReceives)
{
    // TODO: add a timer to make sure the test finish within a time limit
    // Start the emit thread
    std::thread emitThread([this]() { emitRecordsThread(); });

    // Start the poll thread
    std::thread pollThread([this]() {
        int recordCount = pollRecordsThread();
        EXPECT_EQ(recordCount, 10);
    });

    emitThread.join();
    pollThread.join();
}

// Upstream sends too much StreamRecord and causes backpressure
TEST_F(ShuffleEnvironmentTest, DISABLED_UpstreamSendsTooMuchStreamRecordAndCausesBackpressure)
{
    bool downstreamReady = false;

    // Start the emit thread
    std::thread emitThread([this, &downstreamReady]() {
        for (int i = 0; i < 110; i++) {
            if (i == 100) { // hardcoded point of backpressure
                // Notify downstream about the backpressure
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    downstreamReady = true;
                }
                cv_.notify_one();
            }

            upstreamWriter_->emitRecord(dummyRecord_.get(), targetSubpartition_);
        }

        upstreamWriter_->finish(); // send END_OF_PARTITION_EVENT
        upstreamWriter_->close();
    });

    // Start the poll thread
    std::thread pollThread([this, &downstreamReady]() {
        // Wait until the upstream has sent all records
        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [&downstreamReady] { return downstreamReady; });
        }
        int recordCount = pollRecordsThread();
        EXPECT_EQ(recordCount, 110);
    });

    emitThread.join();
    pollThread.join();
}

// Upstream sends end of input before downstream is ready to receive
TEST_F(ShuffleEnvironmentTest, DISABLED_UpstreamSendsEndOfInputBeforeDownstreamIsReadyToReceive)
{
    bool downstreamReady = false;
    int i = 0;

    std::thread emitThread([this, &downstreamReady, &i]() {
        this->emitRecordsThread();
        {
            std::lock_guard<std::mutex> lock(mtx_);
            downstreamReady = true;
        }
        cv_.notify_one();
    });

    std::thread pollThread([this, &downstreamReady]() {
        // Wait until the upstream has sent all records
        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [&downstreamReady] { return downstreamReady; });
        }
        int recordCount = pollRecordsThread();
        EXPECT_EQ(recordCount, 10);
    });

    emitThread.join();
    pollThread.join();
}
