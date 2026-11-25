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
#include <memory>
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"

TEST(SystemProcessingTimeServiceTest, DISABLED_SelectFromRow)
{
    SystemProcessingTimeService systemProcessingTimeService;

    class MockProcessingTimeCallback : public ProcessingTimeCallback {
    public:
        std::vector<long> timeVec;

        void OnProcessingTime(int64_t timestamp) override
        {
            std::cout << std::to_string(timestamp) << std::endl;
            timeVec.push_back(timestamp);
        }
    };

    auto callback = std::make_shared<MockProcessingTimeCallback>();
    systemProcessingTimeService.scheduleWithFixedDelay(callback, 2000, 3000);

    std::this_thread::sleep_for(std::chrono::milliseconds(7900));
    EXPECT_EQ(callback->timeVec.size(), 2);
    EXPECT_EQ(callback->timeVec[0], callback->timeVec[1] - 3000);
}
