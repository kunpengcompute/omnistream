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

#include <string>

#include "runtime/taskexecutor/TaskManagerServices.h"
#include "nlohmann/json.hpp"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "runtime/taskmanager/OmniTask.h"
#include "streaming/runtime/tasks/omni/OmniStreamTask.h"
#include "test/functionaltest/e2e/FrameworkConfig.h"

class TaskManagerTest : public FrameworkTestConfig {
public:
    void SetUp() override
    {
        FrameworkTestConfig::SetUp();

        // TaskManagerServices
        std::ifstream taskMCSConfFile("./input/taskMCSConf.json");
        taskMCSConfJson_ = nlohmann::json::parse(taskMCSConfFile);

        // Source Task
        std::ifstream jobFile("./input/job.json");
        nlohmann::json jobJson = nlohmann::json::parse(jobFile); // same for both source and sink

        std::ifstream sourceTaskFile("./input/sourceTask.json");
        nlohmann::json sourceTaskJson = nlohmann::json::parse(sourceTaskFile);

        std::ifstream sourceTddFile("./input/sourceTdd.json");
        nlohmann::json sourceTddJson = nlohmann::json::parse(sourceTddFile);

        // Sink Task
        std::ifstream sinkTaskFile("./input/sinkTask.json");
        nlohmann::json sinkTaskJson = nlohmann::json::parse(sinkTaskFile);

        std::ifstream sinkTddFile("./input/sinkTdd.json");
        nlohmann::json sinkTddJson = nlohmann::json::parse(sinkTddFile);

        jobInfo_ = jobJson;
        srcTaskInfo_ = sourceTaskJson;
        srcTddInfo_ = sourceTddJson;
        sinkTaskInfo_ = sinkTaskJson;
        sinkTddInfo_ = sinkTddJson;

        clearOutputFile();
    }
};

TEST_F(TaskManagerTest, DISABLED_TaskManagerAndStreamTaskTest)
{
    // TODO: when this test is run together with other test, it passes but will cause other task to fail, there must be
    // memory leak in the entities created in this test JNI createOmniTaskManagerServices
    taskManagerServices_ = std::shared_ptr<omnistream::TaskManagerServices>(
        omnistream::TaskManagerServices::fromConfiguration(taskMCSConfJson_));

    // JNI createNativeTaskExecutor
    taskExecutor_ = std::make_shared<omnistream::OmniTaskExecutor>(taskManagerServices_);

    // JNI submitTaskNative
    sourceTask_ = std::shared_ptr<omnistream::OmniTask>(
        taskExecutor_->submitTask(jobInfo_, srcTaskInfo_, srcTddInfo_, nullptr, nullptr, nullptr, nullptr));
    sinkTask_ = std::shared_ptr<omnistream::OmniTask>(
        taskExecutor_->submitTask(jobInfo_, sinkTaskInfo_, sinkTddInfo_, nullptr, nullptr, nullptr, nullptr));

    std::thread sourceThread([&]() {
        // JNI setupStreamTaskBeforeInvoke
        sourceStreamTask_ = std::shared_ptr<omnistream::OmniStreamTask>(reinterpret_cast<omnistream::OmniStreamTask*>(
            sourceTask_->setupStreamTask(sourceTask_->SOURCE_STREAM_TASK)));
        // JNI doRunNativeTask
        sourceTask_->doRun(reinterpret_cast<long>(sourceStreamTask_.get()));
    });

    std::thread sinkThread([&]() {
        // JNI setupStreamTaskBeforeInvoke
        sinkStreamTask_ = std::shared_ptr<omnistream::OmniStreamTask>(reinterpret_cast<omnistream::OmniStreamTask*>(
            sinkTask_->setupStreamTask(sinkTask_->ONEINTPUT_STREAM_TASK)));
        // JNI doRunNativeTask
        sinkTask_->doRun(reinterpret_cast<long>(sinkStreamTask_.get()));
    });

    sourceThread.join();
    sinkThread.join();

    // Check output
    std::string output = getOutputFile();
    std::string expectedOutput = "+I,1\n+I,4\n+I,7\n+I,10\n";
    EXPECT_EQ(output, expectedOutput);
}
