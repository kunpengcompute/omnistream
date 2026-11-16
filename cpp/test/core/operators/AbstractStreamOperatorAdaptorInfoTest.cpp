/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <stdexcept>

#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "streaming/api/operators/AbstractStreamOperator.h"

// 默认情况下算子未分配 Adaptor：type 为 None，且带有默认 reason。
TEST(AbstractStreamOperatorAdaptorInfoTest, DefaultAdaptorInfoIsNoneWithReason)
{
    auto* op = new AbstractStreamOperator<int>();
    auto info = op->getSavepointAdaptorInfo();
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(info.reason.empty());
}

// setFlinkSavepointAdaptor：设置具体类型后 type 更新、reason 清空，且 getSavepointAdaptorInfo 与之一致。
TEST(AbstractStreamOperatorAdaptorInfoTest, SetAdaptorTypeStoresTypeAndClearsReason)
{
    auto* op = new AbstractStreamOperator<int>();
    op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::GroupAggAdaptor);

    auto info = op->getSavepointAdaptorInfo();
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::GroupAggAdaptor);
    EXPECT_TRUE(info.reason.empty());
}

// setFlinkSavepointAdaptor(None)：应改用 setFlinkSavepointUnsupported，直接传 None 抛异常。
TEST(AbstractStreamOperatorAdaptorInfoTest, SetAdaptorTypeNoneThrows)
{
    auto* op = new AbstractStreamOperator<int>();
    EXPECT_THROW(op->setFlinkSavepointAdaptor(FlinkSavepointAdaptorType::None), std::invalid_argument);
}

// setFlinkSavepointUnsupported：设置 None + 诊断 reason。
TEST(AbstractStreamOperatorAdaptorInfoTest, SetUnsupportedStoresReason)
{
    auto* op = new AbstractStreamOperator<int>();
    op->setFlinkSavepointUnsupported("adaptor not implemented");

    auto info = op->getSavepointAdaptorInfo();
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::None);
    EXPECT_EQ(info.reason, "adaptor not implemented");
}

// setFlinkSavepointUnsupported：reason 不能为空。
TEST(AbstractStreamOperatorAdaptorInfoTest, SetUnsupportedEmptyReasonThrows)
{
    auto* op = new AbstractStreamOperator<int>();
    EXPECT_THROW(op->setFlinkSavepointUnsupported(""), std::invalid_argument);
}
