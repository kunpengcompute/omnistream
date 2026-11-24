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

#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/assigners/SlidingWindowAssigner.h"
#include "table/runtime/operators/window/assigners/TumblingWindowAssigner.h"
#include "table/runtime/operators/window/internal/GeneralWindowProcessFunction.h"
#include "table/runtime/operators/window/internal/PanedWindowProcessFunction.h"

namespace {

using Key = std::shared_ptr<RowData>;

BinaryRowData* NewLongRow(int64_t value)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, value);
    return row;
}

class TestContext : public Context<Key, TimeWindow> {
public:
    explicit TestContext(int64_t watermark = std::numeric_limits<int64_t>::min()) : watermark_(watermark)
    {
    }

    Key currentKey() override
    {
        return currentKey_;
    }

    int64_t currentProcessingTime() override
    {
        return processingTime_;
    }

    int64_t currentWatermark() override
    {
        return watermark_;
    }

    RowData* getWindowAccumulators(const TimeWindow& window) override
    {
        auto it = windowState_.find(window);
        return it == windowState_.end() ? nullptr : it->second.get();
    }

    void setWindowAccumulators(const TimeWindow& window, RowData* accumulators) override
    {
        windowState_[window].reset(accumulators);
    }

    void clearWindowState(const TimeWindow& window) override
    {
        clearedWindowState.push_back(window);
        windowState_.erase(window);
    }

    void clearPreviousState(const TimeWindow& window) override
    {
        clearedPreviousState.push_back(window);
    }

    void clearTrigger(const TimeWindow& window) override
    {
        clearedTriggers.push_back(window);
    }

    void onMerge(const TimeWindow& window, std::vector<TimeWindow>& windows) override
    {
        mergedWindow = window;
        mergedWindows = windows;
    }

    void deleteCleanupTimer(const TimeWindow& window) override
    {
        deletedCleanupTimers.push_back(window);
    }

    void putWindowAccumulator(const TimeWindow& window, int64_t value)
    {
        windowState_[window].reset(NewLongRow(value));
    }

    Key currentKey_;
    int64_t processingTime_ = 0;
    int64_t watermark_;
    std::unordered_map<TimeWindow, std::unique_ptr<RowData>> windowState_;
    std::vector<TimeWindow> clearedWindowState;
    std::vector<TimeWindow> clearedPreviousState;
    std::vector<TimeWindow> clearedTriggers;
    std::vector<TimeWindow> deletedCleanupTimers;
    TimeWindow mergedWindow;
    std::vector<TimeWindow> mergedWindows;
};

class TestAggregator : public NamespaceAggsHandleFunctionBase<TimeWindow> {
public:
    TestAggregator() : NamespaceAggsHandleFunctionBase<TimeWindow>(1)
    {
    }

    void open(StateDataViewStore* store) override
    {
    }

    void setAccumulators(TimeWindow namespaceVal, RowData* accumulators) override
    {
        lastNamespace = namespaceVal;
        currentAccumulators = accumulators;
        setAccumulatorsCalls++;
    }

    void accumulate(RowData* inputRow) override
    {
    }

    void retract(RowData* inputRow) override
    {
    }

    void merge(TimeWindow namespaceVal, RowData* otherAcc) override
    {
        mergedNamespaces.push_back(namespaceVal);
        int64_t otherValue = *otherAcc->getLong(0);
        int64_t currentValue = *currentAccumulators->getLong(0);
        currentAccumulators->setLong(0, currentValue + otherValue);
    }

    RowData* createAccumulators() override
    {
        lastAccumulatorArity = accumulatorArity_;
        createAccumulatorsCalls++;
        return NewLongRow(0);
    }

    RowData* getAccumulators() override
    {
        return currentAccumulators;
    }

    void cleanup(TimeWindow namespaceVal) override
    {
        cleanedNamespaces.push_back(namespaceVal);
    }

    void close() override
    {
    }

    TimeWindow lastNamespace;
    RowData* currentAccumulators = nullptr;
    int32_t lastAccumulatorArity = -1;
    int32_t createAccumulatorsCalls = 0;
    int32_t setAccumulatorsCalls = 0;
    std::vector<TimeWindow> mergedNamespaces;
    std::vector<TimeWindow> cleanedNamespaces;
};

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

} // namespace

TEST(GeneralWindowProcessFunctionTest, AssignsOnlyNonLateStateNamespaces)
{
    TumblingWindowAssigner assigner(5000, 0, true);
    TestAggregator aggregator;
    GeneralWindowProcessFunction<Key, TimeWindow> function(&assigner, &aggregator, 0);

    auto* context = new TestContext(4999);
    function.open(context);

    EXPECT_TRUE(function.assignStateNamespace(nullptr, 4999).empty());
    AssertWindowsEq(function.assignStateNamespace(nullptr, 5000), {TimeWindow(5000, 10000)});
    AssertWindowsEq(function.assignActualWindows(nullptr, 5000), {TimeWindow(5000, 10000)});
}

TEST(GeneralWindowProcessFunctionTest, PreparesExistingAccumulatorAndCleansOnCleanupTime)
{
    TumblingWindowAssigner assigner(5000, 0, true);
    TestAggregator aggregator;
    GeneralWindowProcessFunction<Key, TimeWindow> function(&assigner, &aggregator, 0);

    auto* context = new TestContext(0);
    context->putWindowAccumulator(TimeWindow(0, 5000), 7);
    function.open(context);

    function.prepareAggregateAccumulatorForEmit(TimeWindow(0, 5000));
    ASSERT_NE(aggregator.getAccumulators(), nullptr);
    EXPECT_EQ(*aggregator.getAccumulators()->getLong(0), 7);
    EXPECT_EQ(aggregator.createAccumulatorsCalls, 0);
    EXPECT_EQ(aggregator.lastNamespace, TimeWindow(0, 5000));

    function.cleanWindowIfNeeded(TimeWindow(0, 5000), 4998);
    EXPECT_TRUE(context->clearedWindowState.empty());

    function.cleanWindowIfNeeded(TimeWindow(0, 5000), 4999);
    AssertWindowsEq(context->clearedWindowState, {TimeWindow(0, 5000)});
    AssertWindowsEq(context->clearedPreviousState, {TimeWindow(0, 5000)});
    AssertWindowsEq(context->clearedTriggers, {TimeWindow(0, 5000)});
}

TEST(PanedWindowProcessFunctionTest, AssignsPanesAndActualWindowsUsingFlinkLateSemantics)
{
    SlidingWindowAssigner assigner(5000, 1000, 0, true);
    TestAggregator aggregator;
    PanedWindowProcessFunction<Key, TimeWindow> function(&assigner, &aggregator, 0);

    auto* context = new TestContext(4999);
    function.open(context);

    AssertWindowsEq(function.assignStateNamespace(nullptr, 4999), {TimeWindow(4000, 5000)});
    AssertWindowsEq(
        function.assignActualWindows(nullptr, 4999),
        {TimeWindow(4000, 9000), TimeWindow(3000, 8000), TimeWindow(2000, 7000), TimeWindow(1000, 6000)});

    auto* lateContext = new TestContext(8999);
    PanedWindowProcessFunction<Key, TimeWindow> lateFunction(&assigner, &aggregator, 0);
    lateFunction.open(lateContext);
    EXPECT_TRUE(lateFunction.assignStateNamespace(nullptr, 4999).empty());
}

TEST(PanedWindowProcessFunctionTest, MergesPaneAccumulatorsForEmit)
{
    SlidingWindowAssigner assigner(5000, 1000, 0, true);
    TestAggregator aggregator;
    PanedWindowProcessFunction<Key, TimeWindow> function(&assigner, &aggregator, 0);

    auto* context = new TestContext(0);
    context->putWindowAccumulator(TimeWindow(0, 1000), 1);
    context->putWindowAccumulator(TimeWindow(1000, 2000), 2);
    function.open(context);

    function.prepareAggregateAccumulatorForEmit(TimeWindow(0, 5000));

    ASSERT_NE(aggregator.getAccumulators(), nullptr);
    EXPECT_EQ(*aggregator.getAccumulators()->getLong(0), 3);
    EXPECT_EQ(aggregator.createAccumulatorsCalls, 1);
    EXPECT_EQ(aggregator.lastAccumulatorArity, 1);
    EXPECT_EQ(aggregator.lastNamespace, TimeWindow(0, 5000));
    AssertWindowsEq(aggregator.mergedNamespaces, {TimeWindow(0, 1000), TimeWindow(1000, 2000)});
}

TEST(PanedWindowProcessFunctionTest, CleansPaneStateOnlyWhenWindowIsPaneLastWindow)
{
    SlidingWindowAssigner assigner(5000, 1000, 0, true);
    TestAggregator aggregator;
    PanedWindowProcessFunction<Key, TimeWindow> function(&assigner, &aggregator, 0);

    auto* context = new TestContext(0);
    context->putWindowAccumulator(TimeWindow(0, 1000), 1);
    context->putWindowAccumulator(TimeWindow(1000, 2000), 2);
    function.open(context);

    function.cleanWindowIfNeeded(TimeWindow(0, 5000), 4999);
    AssertWindowsEq(context->clearedWindowState, {TimeWindow(0, 1000)});
    AssertWindowsEq(context->clearedTriggers, {TimeWindow(0, 5000)});
    AssertWindowsEq(context->clearedPreviousState, {TimeWindow(0, 5000)});

    function.cleanWindowIfNeeded(TimeWindow(1000, 6000), 5999);
    AssertWindowsEq(context->clearedWindowState, {TimeWindow(0, 1000), TimeWindow(1000, 2000)});
}
