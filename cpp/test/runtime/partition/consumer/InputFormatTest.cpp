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

#include "gtest/gtest.h"
#include "runtime/partition/consumer/InputFormat.h"

using namespace omnistream;

TEST(InputFormatTest, RejectsConsumptionBeforeDeployment)
{
    InputFormatState state;
    EXPECT_THROW(state.get(), std::logic_error);
}

TEST(InputFormatTest, KeepsDifferentInputsIndependentAndRejectsReconfiguration)
{
    InputFormatState javaInput;
    InputFormatState omniInput;
    javaInput.initialize(InputFormat::FLINK_ROW);
    omniInput.initialize(InputFormat::OMNI_BATCH);
    EXPECT_EQ(javaInput.get(), InputFormat::FLINK_ROW);
    EXPECT_EQ(omniInput.get(), InputFormat::OMNI_BATCH);
    EXPECT_THROW(javaInput.initialize(InputFormat::OMNI_BATCH), std::logic_error);
    EXPECT_THROW(omniInput.initialize(InputFormat::OMNI_BATCH), std::logic_error);
    EXPECT_EQ(javaInput.get(), InputFormat::FLINK_ROW);
}
