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

#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "runtime/checkpoint/StateMetaInfoValidator.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

using omnistream::StateMetaInfoValidator;

namespace {
// 构造一个指定 name、backend 类型与 KEYED_STATE_TYPE 的 StateMetaInfoSnapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeSnapshot(
    const std::string& name, StateMetaInfoSnapshot::BackendStateType type, const std::string& stateType = "VALUE")
{
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = stateType;
    return std::make_shared<StateMetaInfoSnapshot>(
        name, type, options, std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

// 便捷构造一个 KEY_VALUE + VALUE 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeKv(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE");
}

// 便捷构造一个 KEY_VALUE + MAP 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeKvMap(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP");
}

// 便捷构造一个 KEY_VALUE + LIST 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeKvList(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "LIST");
}

// 便捷构造 PRIORITY_QUEUE 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makePq(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE, "");
}

std::shared_ptr<StateMetaInfoSnapshot> makePqTimer(const std::string& suffix = "test")
{
    return makePq("_timer_state/" + suffix);
}
} // namespace

// 构造函数：同名 source state 应当 fail-fast。
TEST(StateMetaInfoValidatorTest, ConstructorRejectsDuplicateNames)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("a"), makeKv("a")};
    EXPECT_THROW(StateMetaInfoValidator{metas}, std::runtime_error);
}

// 构造函数：nullptr 条目应被跳过，不影响其余状态索引。
TEST(StateMetaInfoValidatorTest, ConstructorSkipsNullEntries)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{nullptr, makeKv("a"), nullptr};
    StateMetaInfoValidator validator(metas);
    EXPECT_NO_THROW(validator.requireKeyedValueState("a"));
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requireKeyedValueState：存在的 KV 状态被消费，收口检查通过。
TEST(StateMetaInfoValidatorTest, RequireKeyedValueStateConsumesState)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("a")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requireKeyedValueState("a"));
    EXPECT_EQ(validator.consumed().count("a"), 1u);
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requireKeyedValueState：不存在的状态应抛异常。
TEST(StateMetaInfoValidatorTest, RequireMissingStateThrows)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{};
    StateMetaInfoValidator validator(metas);
    EXPECT_THROW(validator.requireKeyedValueState("missing"), std::runtime_error);
}

// requireKeyedValueState：非 KEY_VALUE 类型（如 OPERATOR）应抛异常。
TEST(StateMetaInfoValidatorTest, RequireNonKeyValueStateThrows)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeSnapshot("a", StateMetaInfoSnapshot::BackendStateType::OPERATOR)};
    StateMetaInfoValidator validator(metas);
    EXPECT_THROW(validator.requireKeyedValueState("a"), std::runtime_error);
}

// requireKeyedMapState / requireKeyedListState：使用正确类型的 snapshot 应通过。
TEST(StateMetaInfoValidatorTest, RequireMapAndListStateBehaveLikeValue)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKvMap("m"), makeKvList("l")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requireKeyedMapState("m"));
    EXPECT_NO_THROW(validator.requireKeyedListState("l"));
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requireStateType 类型不匹配：用 VALUE 类型 snapshot 调 requireKeyedMapState 应抛异常。
TEST(StateMetaInfoValidatorTest, RequireStateTypeMismatchThrows)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("a")};
    StateMetaInfoValidator validator(metas);

    EXPECT_THROW(validator.requireKeyedMapState("a"), std::runtime_error);
    EXPECT_THROW(validator.requireKeyedListState("a"), std::runtime_error);
}

// requireNoMoreStates：存在未被消费的多余状态时应 fail-fast。
TEST(StateMetaInfoValidatorTest, RequireNoMoreStatesDetectsLeftover)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("a"), makeKv("b")};
    StateMetaInfoValidator validator(metas);

    validator.requireKeyedValueState("a");
    EXPECT_THROW(validator.requireNoMoreStates(), std::runtime_error);
}

// requireKeyedValueStateWithVB：主状态与其 VB side table（name + "vb"）应同时被消费。
TEST(StateMetaInfoValidatorTest, RequireWithVBConsumesSideTable)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("s"), makeKv("svb")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requireKeyedValueStateWithVB("s"));
    EXPECT_EQ(validator.consumed().count("s"), 1u);
    EXPECT_EQ(validator.consumed().count("svb"), 1u);
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requireKeyedValueStateWithVB：没有 VB side table 时仍应通过（side table 可选）。
TEST(StateMetaInfoValidatorTest, RequireWithVBWithoutSideTableIsOk)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("s")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requireKeyedValueStateWithVB("s"));
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// get：已索引的 name 返回对应 snapshot，未知 name 抛异常。
TEST(StateMetaInfoValidatorTest, GetReturnsSnapshotOrThrows)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("a")};
    StateMetaInfoValidator validator(metas);

    auto snapshot = validator.get("a");
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->getName(), "a");
    EXPECT_THROW(validator.get("missing"), std::runtime_error);
}

// 批量校验：requireKeyedValueStates/MAP/LIST 全部通过。
TEST(StateMetaInfoValidatorTest, BulkRequireStatesConsumesAll)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKv("va"), makeKv("vb"), makeKvMap("ma"), makeKvList("la")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requireKeyedValueStates({"va", "vb"}));
    EXPECT_NO_THROW(validator.requireKeyedMapStates({"ma"}));
    EXPECT_NO_THROW(validator.requireKeyedListStates({"la"}));
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requirePriorityQueueStates：合法前缀被消费。
TEST(StateMetaInfoValidatorTest, RequirePqConsumesTimerStates)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makePqTimer("a"), makePqTimer("b"), makeKv("s")};
    StateMetaInfoValidator validator(metas);

    EXPECT_NO_THROW(validator.requirePriorityQueueStates());
    EXPECT_EQ(validator.consumed().count("_timer_state/a"), 1u);
    EXPECT_EQ(validator.consumed().count("_timer_state/b"), 1u);
    EXPECT_NO_THROW(validator.requireKeyedValueState("s"));
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}

// requirePriorityQueueStates：非法前缀抛异常。
TEST(StateMetaInfoValidatorTest, RequirePqRejectsNonTimerPrefix)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makePq("bad_timer")};
    StateMetaInfoValidator validator(metas);

    EXPECT_THROW(validator.requirePriorityQueueStates(), std::runtime_error);
}

// 构造函数：空 vector 构造 validator 后 requireNoMoreStates 应通过。
TEST(StateMetaInfoValidatorTest, EmptyValidatorPassesNoMoreStates)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{};
    StateMetaInfoValidator validator(metas);
    EXPECT_NO_THROW(validator.requireNoMoreStates());
}
