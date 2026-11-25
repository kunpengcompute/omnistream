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

#include "streaming/api/environment/ExecutionCheckpointingOptions.h"
#include <gtest/gtest.h>

TEST(ExecutionCheckpointingOptionsTest, StaticInstanceTest)
{
    EXPECT_EQ(
        ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->key(),
        "execution.checkpointing.checkpoints-after-tasks-finish.enabled");
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->hasDefaultValue(), true);
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->defaultValue(), false);
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->hasFallbackKeys(), false);

    EXPECT_EQ(ExecutionCheckpointingOptions::ALIGNED_CHECKPOINT_TIMEOUT->hasFallbackKeys(), true);
}
