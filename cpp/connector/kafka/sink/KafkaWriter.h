/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNIFLINK_KAFKAWRITER_H
#define OMNIFLINK_KAFKAWRITER_H

#include <deque>
#include <exception>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

#include <librdkafka/rdkafkacpp.h>
#include "KafkaCommittable.h"
#include "KafkaWriterState.h"
#include "TransactionAborter.h"
#include "KafkaRecordSerializationSchema.h"
#include "DynamicKafkaRecordSerializationSchema.h"
#include "TransactionalIdFactory.h"
#include "core/include/common.h"
#include "table/data/binary/BinaryRowData.h"
#include "DeliveryGuarantee.h"
#include "atomic"
#include "table/data/vectorbatch/VectorBatch.h"
#include "include/common.h"
#include "../bind_core_manager.h"
#include "api/common/TimerThreadPool.h"
#include "streaming/api/operators/sink/InitContextImpl.h"

class KafkaWriter {
public:
    KafkaWriter(
        DeliveryGuarantee deliveryGuarantee,
        RdKafka::Conf* kafkaProducerConfig,
        std::string& transactionalIdPrefix,
        std::string& topic,
        const nlohmann::json& description,
        int64_t maxPushRecords,
        InitContextImpl<void*>* initContext,
        const std::vector<KafkaWriterState>& states);
    ~KafkaWriter();

    void write(String* element);
    void write(Row* element);
    void write(RowData* element);
    void write(omnistream::VectorBatch* input, int rowIndex);

    void produce(
        RdKafka::Producer* kafkaProducer,
        RdKafka::Topic* rd_topic,
        const std::vector<char*>& value,
        const std::vector<size_t>& valuesLen);

    void Flush(bool endOfInput);
    std::vector<KafkaCommittable> prepareCommit();
    std::vector<KafkaWriterState> recoveredStates;
    RdKafka::Conf* kafkaProducerConfig = nullptr;
    std::string topic;
    KafkaWriterState* kafkaWriterState = nullptr;

    void SetSubTaskIdx(int32_t subtaskIdx);
    std::vector<KafkaWriterState> snapshotState(long checkpointId);

private:
    static constexpr const char* KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    static constexpr long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    static constexpr const char* KEY_DISABLE_METRICS = "flink.disable-metrics";
    static constexpr const char* KEY_REGISTER_METRICS = "register.producer.metrics";
    static constexpr const char* KAFKA_PRODUCER_METRICS = "producer-metrics";

    DeliveryGuarantee deliveryGuarantee;
    std::string transactionalIdPrefix = "kafka-sink";
    DynamicKafkaRecordSerializationSchema* recordSerializer = nullptr;
    std::exception_ptr asyncProducerException;
    nlohmann::json description;
    bool upsertMode;
    std::vector<std::string> inputFields;
    std::vector<std::string> inputTypes;

    long latestOutgoingByteTotal{};
    std::shared_ptr<FlinkKafkaInternalProducer> currentProducer1;
    std::shared_ptr<FlinkKafkaInternalProducer> currentProducer2;
    std::deque<std::shared_ptr<FlinkKafkaInternalProducer>> producerPool;
    long lastCheckpointId{};

    bool closed = false;
    long lastSync = 0;

    std::vector<char*> values;
    std::vector<size_t> valuesLens;
    int limit = 10000; // 10000
    int cur = 0;

    std::thread worker_thread;               // 后台工作线程
    std::mutex queueMutex;                   // 任务队列互斥锁
    std::condition_variable cv;              // 条件变量（用于唤醒线程）
    std::queue<std::function<void()>> tasks; // 任务队列
    std::atomic<bool> stop_flag{false};      // 停止标志

    std::condition_variable tasksDrainedCv;
    size_t inFlightTasks = 0;

    std::mutex gMtx;
    std::condition_variable gcv;
    timespec start;
    timespec end;

    std::atomic<bool> timer_worker_thread_flag{true};

    void AbortCurrentProducer();
    void abortLingeringTransactions(const std::vector<KafkaWriterState>& recoveredStates, long startCheckpointId);
    std::shared_ptr<FlinkKafkaInternalProducer> getTransactionalProducer(long checkpointId);
    std::shared_ptr<FlinkKafkaInternalProducer> getOrCreateTransactionalProducer(const std::string& transactionalId);
    void ProduceRecord(KeyValueByteContainer& record);
    void handleRecord();
    void waitForPendingTasks();
    void checkAsyncProducerException();
    RdKafka::Topic* rd_topic1 = nullptr;
    RdKafka::Topic* rd_topic2 = nullptr;
    int32_t instanceId = 0;
    int producerIndexOne = 1;
    int producerIndexTwo = 2;
    int32_t bindCore = -1;
    bool binded = false;
    omnistream::TimerThreadPool::TaskId taskId;

    void Init()
    {
        instanceId = getInstanceId();
        values.reserve(limit);
        valuesLens.reserve(limit);
    };

    void timer_thread()
    {
        if (!timer_worker_thread_flag.load()) {
            return;
        }
        std::unique_lock<std::mutex> gLock(gMtx);
        if (!timer_worker_thread_flag.load()) {
            return;
        }
        try {
            handleRecord();
        } catch (...) {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (asyncProducerException == nullptr) {
                asyncProducerException = std::current_exception();
            }
        }
    }

    void WorkerThreadFunc()
    {
        if (bindCore >= 0) {
            omnistream::BindCoreManager::GetInstance()->BindDirectCore(bindCore);
        }
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                // 等待任务或停止信号
                cv.wait(lock, [this]() { return !tasks.empty() || stop_flag.load(); });

                // 如果收到停止信号且队列为空，退出线程
                if (stop_flag.load() && tasks.empty()) {
                    return;
                }

                // 取出任务
                task = std::move(tasks.front());
                tasks.pop();
                ++inFlightTasks;
            }

            // 执行任务
            auto completeTask = [this](std::exception_ptr producerException) {
                std::lock_guard<std::mutex> lock(queueMutex);
                if (producerException != nullptr && asyncProducerException == nullptr) {
                    asyncProducerException = producerException;
                }
                --inFlightTasks;
                if (tasks.empty() && inFlightTasks == 0) {
                    tasksDrainedCv.notify_all();
                }
            };
            try {
                task();
            } catch (...) {
                completeTask(std::current_exception());
                continue;
            }
            completeTask(nullptr);
        }
    }

    static int32_t getInstanceId()
    {
        static std::atomic<int32_t> instanceIdAtomic = 0;
        auto retId = instanceIdAtomic.fetch_add(1, std::memory_order_seq_cst);
        return retId;
    }
};

#endif // OMNIFLINK_KAFKAWRITER_H
