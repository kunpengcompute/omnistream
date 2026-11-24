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

#include "streaming/runtime/partitioner/V2/ForwardPartitionerV2.h"
#include "streaming/runtime/partitioner/V2/RebalancePartitionerV2.h"
#include "streaming/runtime/partitioner/V2/RescalePartitionerV2.h"
#include "streaming/runtime/partitioner/V2/StreamPartitionerV2.h"
#include "core/writer/vectorBatchTestUtils.h"
#include "nlohmann/json.hpp"
#include "runtime/executiongraph/StreamPartitionerPOD.h"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "runtime/taskmanager/OmniTask.h"
#include "streaming/runtime/tasks/omni/OmniStreamTask.h"

class FrameworkTestConfig : public ::testing::Test {
public:
    void SetUp() override
    {
        clearOutputFile();
    }

    void TearDown() override
    {
        clearOutputFile();
        ::testing::Test::TearDown();
    }

    static void clearOutputFile()
    {
        std::ofstream ofs("/tmp/flink_output.txt", std::ofstream::out | std::ofstream::trunc);
        ofs.close();
    }

    static std::string getOutputFile()
    {
        std::ifstream ifs("/tmp/flink_output.txt");
        std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        return content;
    }

    static std::string getPartitionerName(const omnistream::TaskInformationPOD& taskInfo)
    {
        return taskInfo.getStreamConfigPOD().getOutEdgesInOrder()[0].getPartitioner().getPartitionerName();
    }

    /**
     * Helper funciton to create partitioner from partitioner name, copied from `runtime/tasks/OmniStreamTask.cpp`
     */
    static std::shared_ptr<omnistream::StreamPartitionerV2<StreamRecord>> getPartitionerFromPartitionerName(
        const std::string& partitionerName)
    {
        if (partitionerName == omnistream::StreamPartitionerPOD::FORWARD) {
            return std::make_shared<omnistream::ForwardPartitionerV2<StreamRecord>>();
        } else if (partitionerName == omnistream::StreamPartitionerPOD::REBALANCE) {
            return std::make_shared<omnistream::RebalancePartitionerV2<StreamRecord>>();
        } else if (partitionerName == omnistream::StreamPartitionerPOD::RESCALE) {
            return std::make_shared<omnistream::RescalePartitionerV2<StreamRecord>>();
        } else {
            throw std::invalid_argument("Invalid partitioner!");
        }
    }

    static std::shared_ptr<StreamRecord> createDummyRecord(
        int numRows, std::vector<omniruntime::type::DataTypeId>& types)
    {
        auto vectorBatch = omnistream::createVectorBatch(types, numRows, true);
        // omniruntime::vec::VectorHelper::PrintVecBatch(vectorBatch);
        return std::make_shared<StreamRecord>(vectorBatch);
    }

protected:
    std::shared_ptr<omnistream::TaskManagerServices> taskManagerServices_;
    std::shared_ptr<omnistream::OmniTaskExecutor> taskExecutor_;
    std::shared_ptr<omnistream::OmniTask> sourceTask_;
    std::shared_ptr<omnistream::OmniTask> sinkTask_;
    std::shared_ptr<omnistream::OmniStreamTask> sourceStreamTask_;
    std::shared_ptr<omnistream::OmniStreamTask> sinkStreamTask_;

    nlohmann::json taskMCSConfJson_;
    omnistream::JobInformationPOD jobInfo_;
    omnistream::TaskInformationPOD srcTaskInfo_;
    omnistream::TaskDeploymentDescriptorPOD srcTddInfo_;
    omnistream::TaskInformationPOD sinkTaskInfo_;
    omnistream::TaskDeploymentDescriptorPOD sinkTddInfo_;
};
