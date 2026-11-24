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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>
#include "datagen/nexmark/generator/NexmarkGenerator.h"
#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/DefaultOperatorStateBackend.h"
#include "runtime/state/DefaultOperatorStateBackendBuilder.h"
#include "runtime/state/StateInitializationContextImpl.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"

class DummySourceContext : public SourceContext {
public:
    DummySourceContext(Object* lock) : lock(lock)
    {
    }

    void collect(void* element) override
    {
        {
            std::lock_guard<std::mutex> guard(collectMutex);
            reUseRecord = element;
            hasCollected = true;
            collectCount++;
        }
        collectCondition.notify_all();
    }

    void collectWithTimestamp(void* element, int64_t timestamp) override
    {
        // ignore the timestamp
        collect(element);
    }

    void emitWatermark(Watermark* mark) override
    {
        // do nothing
    }

    void markAsTemporarilyIdle() override
    {
        // do nothing
    }

    Object* getCheckpointLock() override
    {
        return lock;
    }

    void close() override
    {
    }

    bool waitForCollect(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> guard(collectMutex);
        return collectCondition.wait_for(guard, timeout, [this]() { return hasCollected; });
    }

    int getCollectCount() const
    {
        return collectCount;
    }

    void* reUseRecord;
    Object* lock;

private:
    std::mutex collectMutex;
    std::condition_variable collectCondition;
    bool hasCollected = false;
    int collectCount = 0;
};

namespace {
std::unique_ptr<DefaultOperatorStateBackend> CreateOperatorStateBackend()
{
    DefaultOperatorStateBackendBuilder builder(false, "nexmark-source-test", {}, nullptr, nullptr);
    return std::unique_ptr<DefaultOperatorStateBackend>(static_cast<DefaultOperatorStateBackend*>(builder.build()));
}

std::shared_ptr<ListState<long>> GetNexmarkOffsetState(DefaultOperatorStateBackend* backend)
{
    std::string stateName = "elements-count-state";
    auto* descriptor = new ListStateDescriptor<long>(stateName, new LongSerializer());
    return backend->getListState<long>(descriptor);
}

void SetRuntimeContext(NexmarkSourceFunction<omnistream::VectorBatch>& source)
{
    auto* runtimeEnv = new omnistream::RuntimeEnvironmentV2();
    auto* runtimeCtx = new StreamingRuntimeContext<int>();
    runtimeCtx->setEnvironment(runtimeEnv);
    source.setRuntimeContext(runtimeCtx);
}
} // namespace

TEST(SourceTest, NexmarkDataGeneratorTest)
{
    int batchSize = 100;
    BatchEventDeserializer* eventDeserializer = new BatchEventDeserializer(batchSize);
    auto typeInfo = TypeInfoFactory::createTypeInfo("String");

    // In NexmarkConfiguration, all values have been set to their default value
    NexmarkConfiguration nexmarkConfig;

    /*
     * const NexmarkConfiguration &configuration,
                int64_t baseTime,
                int64_t firstEventId,
                int64_t maxEventsOrZero,
                int64_t firstEventNumber
    */
    std::string baseTimeStr = "2025-02-22 00:00:00";
    GeneratorConfig config{nexmarkConfig, 1740182400000, 0, 100, 0};
    NexmarkSourceFunction<omnistream::VectorBatch> srcFunc{config, eventDeserializer, typeInfo};

    auto runtimeEnv = new omnistream::RuntimeEnvironmentV2();
    auto runtimeCtx = new StreamingRuntimeContext<int>();
    runtimeCtx->setEnvironment(runtimeEnv);
    srcFunc.setRuntimeContext(runtimeCtx);
    Configuration runConfig;
    srcFunc.open(runConfig);
    thread_local Object lock;
    DummySourceContext ctx(&lock);
    srcFunc.run(&ctx);
    auto output = reinterpret_cast<omnistream::VectorBatch*>(ctx.reUseRecord);
    EXPECT_EQ(output->GetRowCount(), batchSize);
}

TEST(SourceTest, NexmarkCancelInterruptsEventRateWait)
{
    constexpr int batchSize = 1;
    auto* eventDeserializer = new BatchEventDeserializer(batchSize);
    auto* typeInfo = TypeInfoFactory::createTypeInfo("String");

    NexmarkConfiguration nexmarkConfig;
    nexmarkConfig.firstEventRate = 1;
    nexmarkConfig.nextEventRate = 1;
    nexmarkConfig.numEventGenerators = 10;
    GeneratorConfig config{nexmarkConfig, 1740182400000, 0, 100, 0};
    NexmarkSourceFunction<omnistream::VectorBatch> srcFunc{config, eventDeserializer, typeInfo};

    auto* runtimeEnv = new omnistream::RuntimeEnvironmentV2();
    auto* runtimeCtx = new StreamingRuntimeContext<int>();
    runtimeCtx->setEnvironment(runtimeEnv);
    srcFunc.setRuntimeContext(runtimeCtx);
    Configuration runConfig;
    srcFunc.open(runConfig);

    thread_local Object lock;
    DummySourceContext ctx(&lock);
    std::thread sourceThread([&srcFunc, &ctx]() { srcFunc.run(&ctx); });

    if (!ctx.waitForCollect(std::chrono::seconds(2))) {
        srcFunc.cancel();
        sourceThread.join();
        FAIL() << "Nexmark source did not emit its first event";
        return;
    }

    // The next event is ten seconds away. Allow the source thread to enter its rate wait before cancellation.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto cancelStart = std::chrono::steady_clock::now();
    srcFunc.cancel();
    sourceThread.join();
    const auto cancelDuration = std::chrono::steady_clock::now() - cancelStart;

    EXPECT_LT(cancelDuration, std::chrono::seconds(1));
}

TEST(SourceTest, NexmarkExhaustedGeneratorEmitsPartialBatch)
{
    constexpr int batchSize = 10;
    constexpr int maxEvents = 3;
    NexmarkConfiguration nexmarkConfig;
    GeneratorConfig config{nexmarkConfig, 1740182400000, 0, maxEvents, 0};
    NexmarkSourceFunction<omnistream::VectorBatch> source{
        config, new BatchEventDeserializer(batchSize), TypeInfoFactory::createTypeInfo("String")};
    SetRuntimeContext(source);

    auto backend = CreateOperatorStateBackend();
    StateInitializationContextImpl initializationContext(
        std::optional<uint64_t>{}, backend.get(), static_cast<DefaultKeyedStateStore<int>*>(nullptr));
    source.initializeState(&initializationContext);
    source.open(Configuration());

    thread_local Object lock;
    DummySourceContext sourceContext(&lock);
    source.run(&sourceContext);
    source.snapshotState(nullptr);

    auto values = GetNexmarkOffsetState(backend.get())->get();
    ASSERT_EQ(values->size(), 1);
    EXPECT_EQ(values->at(0), maxEvents);
    EXPECT_EQ(sourceContext.getCollectCount(), 1);
    auto* output = reinterpret_cast<omnistream::VectorBatch*>(sourceContext.reUseRecord);
    EXPECT_EQ(output->GetRowCount(), maxEvents);
}

TEST(SourceTest, NexmarkOpenPreservesRestoredGeneratorOffset)
{
    constexpr int batchSize = 10;
    NexmarkConfiguration nexmarkConfig;
    GeneratorConfig config{nexmarkConfig, 1740182400000, 0, 20, 0};
    NexmarkSourceFunction<omnistream::VectorBatch> source{
        config, new BatchEventDeserializer(batchSize), TypeInfoFactory::createTypeInfo("String")};
    SetRuntimeContext(source);

    auto backend = CreateOperatorStateBackend();
    auto offsetState = GetNexmarkOffsetState(backend.get());
    long restoredOffset = 10;
    offsetState->add(restoredOffset);
    StateInitializationContextImpl initializationContext(
        std::optional<uint64_t>{1}, backend.get(), static_cast<DefaultKeyedStateStore<int>*>(nullptr));

    source.initializeState(&initializationContext);
    source.open(Configuration());

    thread_local Object lock;
    DummySourceContext sourceContext(&lock);
    source.run(&sourceContext);
    source.snapshotState(nullptr);

    auto values = offsetState->get();
    ASSERT_EQ(values->size(), 1);
    EXPECT_EQ(values->at(0), 20);
    EXPECT_EQ(sourceContext.getCollectCount(), 1);
}
