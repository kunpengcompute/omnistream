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

#include "datagen/meituan/JoinSource.h"
#include "test/core/operators/OutputTest.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/source/NonTimestampContext.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include <gtest/gtest.h>

TEST(DatagenTest, MeituanTest)
{
    // Set up runtime context
    auto runtimeEnv = new omnistream::RuntimeEnvironmentV2();
    auto runtimeCtx = new StreamingRuntimeContext<StreamRecord>();
    runtimeCtx->setEnvironment(runtimeEnv);

    // Initialize Source
    JoinSource joinSource(100, 0, 1, 10, 1, 10, 10, 10, 10, 10, 1000);
    joinSource.setRuntimeContext(runtimeCtx);
    joinSource.open(Configuration());

    // Set up  SourceContext and output
    auto output = new OutputTestVectorBatch();
    thread_local Object lockingObject;
    auto ctx = new NonTimestampContext(&lockingObject, output);

    // Running and stopping source
    std::thread sourceThread([&]() { joinSource.run(ctx); });
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    sourceThread.join();

    // Output records
    auto outputRecords = output->getAll();
    EXPECT_FALSE(outputRecords.empty());
    for (const auto& record : outputRecords) {
        EXPECT_TRUE(record->GetRowCount() > 0);
    }

    std::cout << std::endl;

    // Records not outputted yet
    auto records = joinSource.getRecordsToCollect();
    EXPECT_FALSE(records.empty());
    for (const auto& entry : records) {
        EXPECT_FALSE(entry.second.empty());
        for (const auto& record : entry.second) {
            EXPECT_TRUE(record->getTimestamp() > 0);
            EXPECT_FALSE(record->getValue().empty());
        }
    }
}
