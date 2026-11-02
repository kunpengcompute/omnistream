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

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <nlohmann/json.hpp>
#include "streaming/api/functions/sink/filesystem/StreamingFileWriter.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "table/typeutils/RowDataSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"

const std::string baseTempPath = fs::temp_directory_path().string();

std::string prepareTestDirectory(const std::string& basePath = baseTempPath)
{
    auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string path = basePath + "/" + std::string(testInfo->test_suite_name()) + "." + testInfo->name();
    fs::create_directories(path);

    for (const auto& entry : fs::directory_iterator(path)) {
        fs::remove_all(entry);
    }
    return path;
}

std::string readFile(const std::string& filename, std::size_t lastNLines = 0)
{
    std::ifstream in(filename);
    if (lastNLines > 0) {
        std::deque<std::string> tail;
        std::string line;
        while (std::getline(in, line)) {
            tail.push_back(std::move(line));
            if (tail.size() > lastNLines) tail.pop_front();
        }
        std::ostringstream oss;
        for (auto& l : tail) {
            oss << l << '\n';
        }
        return oss.str();
    } else {
        std::ostringstream oss;
        oss << in.rdbuf();
        return oss.str();
    }
}

nlohmann::json fileSinkDesc = nlohmann::json::parse(R"delim({
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "STRING", "STRING"],
        "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "STRING", "STRING"],
        "partition": {
            "time-extractor": { "timestamp-pattern": "$dt $hm:00" }
        },
        "partition-commit": {
            "delay": "5 min",
            "policy": { "kind": "success-file" },
            "trigger": "partition-time"
        },
        "partitionKeys": ["dt", "hm"],
        "partitionIndexes": [5, 6],
        "nonPartitionIndexes": [0, 1, 2, 3, 4],
        "rolling-policy": {
            "check-interval": "1min",
            "rollover-interval": "1min",
            "file-size": ""
        }
    })delim");

TEST(StreamingFileWriterTest, ProcessElement)
{
    std::string pathDir = prepareTestDirectory();

    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);

    std::vector<std::string> partitionKeys = {"dt", "hm"};
    std::vector<int> partitionIndexes = {5, 6};
    std::vector<int> nonPartitionIndexes = {0, 1, 2, 3, 4};
    auto* rollingPolicy = new RollingPolicy<omnistream::VectorBatch*, std::string>(10000, 10000, 10000);
    auto* bucketAssigner = new BucketAssigner<omnistream::VectorBatch*, std::string>(partitionKeys, partitionIndexes);
    auto* bucketFactory = new BucketFactory<omnistream::VectorBatch*, std::string>();
    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(
        Path(pathDir),
        bucketAssigner,
        rollingPolicy,
        bucketFactory,
        OutputFileConfig("output", ".txt"),
        nonPartitionIndexes);

    auto writer = new StreamingFileWriter<omnistream::VectorBatch*>(1000, bucketsBuilder);
    writer->setProcessingTimeService(new SystemProcessingTimeService());

    writer->setup();
    ASSERT_NO_THROW(writer->initializeState(initializer, ser));

    int nrow = 4;
    std::vector<long> col0 = {10, 20, 30, 40};
    std::vector<long> col1 = {11, 21, 31, 41};
    std::vector<long> col2 = {12, 22, 32, 42};
    std::vector<long> col3 = {13, 23, 33, 43};
    std::vector<long> col4 = {14, 24, 34, 44};
    std::vector<std::string> col5 = {"2025-01-15", "2025-01-15", "2025-05-30", "2025-01-15"};
    std::vector<std::string> col6 = {"20:21", "10:10", "10:15", "20:21"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col4.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col5.data(), nrow));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col6.data(), nrow));

    StreamRecord* record = new StreamRecord(reinterpret_cast<void*>(vb));
    writer->processBatch(record);
    writer->endInput();

    std::string file1 = pathDir + "/dt=2025-01-15/hm=20%3A21/output-0-0.txt";
    std::string file2 = pathDir + "/dt=2025-01-15/hm=10%3A10/output-0-1.txt";
    std::string file3 = pathDir + "/dt=2025-05-30/hm=10%3A15/output-0-2.txt";

    EXPECT_TRUE(fs::exists(file1));
    EXPECT_TRUE(fs::exists(file2));
    EXPECT_TRUE(fs::exists(file3));

    EXPECT_EQ(
        readFile(file1),
        "10,11,12,13,14\n"
        "40,41,42,43,44\n");

    EXPECT_EQ(readFile(file2), "20,21,22,23,24\n");

    EXPECT_EQ(readFile(file3), "30,31,32,33,34\n");
}

TEST(StreamingFileWriterTest, RollingPolicyFileSize)
{
    std::string pathDir = prepareTestDirectory();

    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);

    int fileSize = 100;
    std::vector<std::string> partitionKeys = {"dt", "hm"};
    std::vector<int> partitionIndexes = {5, 6};
    std::vector<int> nonPartitionIndexes = {0, 1, 2, 3, 4};
    auto* rollingPolicy = new RollingPolicy<omnistream::VectorBatch*, std::string>(fileSize, 10000, 10000);
    auto* bucketAssigner = new BucketAssigner<omnistream::VectorBatch*, std::string>(partitionKeys, partitionIndexes);
    auto* bucketFactory = new BucketFactory<omnistream::VectorBatch*, std::string>();
    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(
        Path(pathDir),
        bucketAssigner,
        rollingPolicy,
        bucketFactory,
        OutputFileConfig("output", ".txt"),
        nonPartitionIndexes);

    auto writer = new StreamingFileWriter<omnistream::VectorBatch*>(1000, bucketsBuilder);
    writer->setProcessingTimeService(new SystemProcessingTimeService());

    writer->setup();
    ASSERT_NO_THROW(writer->initializeState(initializer, ser));

    int nrow = 500;
    std::vector<long> col0, col1, col2, col3, col4;
    std::vector<std::string> col5, col6;

    for (int i = 0; i < nrow; ++i) {
        col0.push_back(10 + i * 10);
        col1.push_back(11 + i * 10);
        col2.push_back(12 + i * 10);
        col3.push_back(13 + i * 10);
        col4.push_back(14 + i * 10);
        col5.push_back("2025-01-15");
        col6.push_back("20:21");
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col4.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col5.data(), nrow));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col6.data(), nrow));

    StreamRecord* record = new StreamRecord(reinterpret_cast<void*>(vb));
    writer->processBatch(record);
    writer->endInput();

    for (int i = 0; i <= 96; ++i) {
        std::string filePath = pathDir + "/dt=2025-01-15/hm=20%3A21/output-0-" + std::to_string(i) + ".txt";
        EXPECT_TRUE(fs::exists(filePath));
    }

    std::string lastFilePath = pathDir + "/dt=2025-01-15/hm=20%3A21/output-0-96.txt";
    EXPECT_EQ(
        readFile(lastFilePath),
        "4980,4981,4982,4983,4984\n"
        "4990,4991,4992,4993,4994\n"
        "5000,5001,5002,5003,5004\n");
}

TEST(StreamingFileWriterTest, DISABLED_RollingPolicyTimeInterval)
{
    std::string pathDir = prepareTestDirectory();

    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);

    int rollingTimeInterval = 10; // 10ms, should be short enought to produce at least 2 files.
    int bucketCheckInterval = 100;
    std::vector<std::string> partitionKeys = {"dt", "hm"};
    std::vector<int> partitionIndexes = {5, 6};
    std::vector<int> nonPartitionIndexes = {0, 1, 2, 3, 4};
    auto* rollingPolicy = new RollingPolicy<omnistream::VectorBatch*, std::string>(1000000, rollingTimeInterval, 10000);
    auto* bucketAssigner = new BucketAssigner<omnistream::VectorBatch*, std::string>(partitionKeys, partitionIndexes);
    auto* bucketFactory = new BucketFactory<omnistream::VectorBatch*, std::string>();
    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(
        Path(pathDir),
        bucketAssigner,
        rollingPolicy,
        bucketFactory,
        OutputFileConfig("output", ".txt"),
        nonPartitionIndexes);

    auto writer = new StreamingFileWriter<omnistream::VectorBatch*>(bucketCheckInterval, bucketsBuilder);
    writer->setProcessingTimeService(new SystemProcessingTimeService());

    writer->setup();
    ASSERT_NO_THROW(writer->initializeState(initializer, ser));

    int nrow = 20000;
    std::vector<long> col0, col1, col2, col3, col4;
    std::vector<std::string> col5, col6;

    for (int i = 0; i < nrow; ++i) {
        col0.push_back(10 + i * 10);
        col1.push_back(11 + i * 10);
        col2.push_back(12 + i * 10);
        col3.push_back(13 + i * 10);
        col4.push_back(14 + i * 10);
        col5.push_back("2025-01-15");
        col6.push_back("20:21");
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col4.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col5.data(), nrow));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col6.data(), nrow));

    StreamRecord* record = new StreamRecord(reinterpret_cast<void*>(vb));
    writer->processBatch(record);
    writer->endInput();

    auto base = pathDir + "/dt=2025-01-15/hm=20%3A21/";
    int fileCount = 0;

    for (int i = 0; i < 10; ++i) {
        std::string filename = base + "output-0-" + std::to_string(i) + ".txt";
        if (fs::exists(filename)) {
            ++fileCount;
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    EXPECT_GE(fileCount, 2);
}

TEST(StreamingFileWriterTest, Q10)
{
    std::string pathDir = prepareTestDirectory();
    fileSinkDesc["path"] = pathDir;
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);

    std::vector<std::string> partitionKeys = fileSinkDesc["partitionKeys"].get<std::vector<std::string>>();
    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(fileSinkDesc);
    int bucketCheckInterval = 1000;

    auto writer = new StreamingFileWriter<omnistream::VectorBatch*>(bucketCheckInterval, bucketsBuilder);
    writer->setProcessingTimeService(new SystemProcessingTimeService());

    writer->setup();
    ASSERT_NO_THROW(writer->initializeState(initializer, ser));

    int nrow = 20000;
    std::vector<long> col0, col1, col2, col3, col4;
    std::vector<std::string> col5, col6;

    for (int i = 0; i < nrow; ++i) {
        col0.push_back(10 + i * 10);
        col1.push_back(11 + i * 10);
        col2.push_back(12 + i * 10);
        col3.push_back(13 + i * 10);
        col4.push_back(14 + i * 10);
        col5.push_back("2025-01-15");
        col6.push_back("20:21");
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col4.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col5.data(), nrow));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col6.data(), nrow));

    StreamRecord* record = new StreamRecord(reinterpret_cast<void*>(vb));
    writer->processBatch(record);
    writer->endInput();

    std::string outputFilePath = pathDir + "/dt=2025-01-15/hm=20%3A21/output-0-0.txt";
    EXPECT_EQ(
        readFile(outputFilePath, 3),
        "199980,199981,199982,199983,199984\n"
        "199990,199991,199992,199993,199994\n"
        "200000,200001,200002,200003,200004\n");
}
