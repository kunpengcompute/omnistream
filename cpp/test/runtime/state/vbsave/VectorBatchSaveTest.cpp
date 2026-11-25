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

#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"

namespace {
class MockSerializer : public TypeSerializer {};
} // namespace

TEST(VectorBatchSaveTest, DetectsOnlyNamesEndingWithVb)
{
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::isVbStateName("statevb"));
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::isVbStateName("vb"));

    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName(""));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("v"));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("vbState"));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("stateVB"));
}

TEST(VectorBatchSaveTest, SaveStateContextRequiresSerializerForTransformedStates)
{
    MockSerializer serializer;
    omnistream::VectorBatchSaveStateContext context;
    EXPECT_FALSE(context.isValid());

    context.writable = true;
    EXPECT_FALSE(context.isValid());

    context.mappedKvStateId = 0;
    EXPECT_TRUE(context.isValid());

    context.stateType = omnistream::VectorBatchStateType::PQ;
    EXPECT_TRUE(context.isValid());

    context.stateType = omnistream::VectorBatchStateType::KV_TRANSFORM;
    EXPECT_FALSE(context.isValid());

    context.valueSerializer = &serializer;
    EXPECT_TRUE(context.isValid());

    context.writable = false;
    EXPECT_FALSE(context.isValid());
}

TEST(VectorBatchSaveTest, PlanDefaultsAreEmptyAndUnset)
{
    omnistream::VectorBatchSavePlan plan;

    EXPECT_TRUE(plan.targetMetaInfos.empty());
    EXPECT_TRUE(plan.mainStateIds.empty());
    EXPECT_TRUE(plan.kvStateIdMapping.empty());
    EXPECT_TRUE(plan.stateContextSpecs.empty());
    EXPECT_EQ(plan.keyGroupRange, nullptr);
    EXPECT_TRUE(plan.keySerializerJson.empty());
}

TEST(VectorBatchSaveTest, StateContextSpecCarriesSourceState)
{
    MockSerializer serializer;
    omnistream::VectorBatchSavePlan::StateContextSpec spec;
    spec.sourceKvStateId = 7;
    spec.logicalStateName = "orders";
    spec.valueSerializer = &serializer;

    EXPECT_EQ(spec.sourceKvStateId, 7);
    EXPECT_EQ(spec.logicalStateName, "orders");
    EXPECT_EQ(spec.valueSerializer, &serializer);
}
