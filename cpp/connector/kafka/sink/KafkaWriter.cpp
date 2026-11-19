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

#include "KafkaWriter.h"
#include <chrono>
#include <cstddef>
#include "../bind_core_manager.h"
#include "include/common.h"

KafkaWriter::KafkaWriter(
    DeliveryGuarantee deliveryGuarantee,
    RdKafka::Conf* kafkaProducerConfig,
    std::string& transactionalIdPrefix,
    std::string& topic,
    const nlohmann::json& description,
    int64_t maxPushRecords,
    InitContextImpl<void*>* initContext,
    const std::vector<KafkaWriterState>& states)
    : kafkaProducerConfig(kafkaProducerConfig),
      topic(topic),
      deliveryGuarantee(deliveryGuarantee),
      transactionalIdPrefix(transactionalIdPrefix),
      description(description),
      upsertMode(description.value("upsertMode", false)),
      limit(maxPushRecords)
{
    Init();
    if (deliveryGuarantee == DeliveryGuarantee::EXACTLY_ONCE) {
        const std::string errorMessage =
            "Error: KafkaWriter::KafkaWriter -> delivery guarantee is not supported deliveryGuarantee=EXACTLY_ONCE";
        INFO_RELEASE(errorMessage);
        throw std::invalid_argument("Kafka sink EXACTLY_ONCE delivery guarantee is not supported");
    }
    if (description.value("batch", false)) {
        if (upsertMode) {
            recordSerializer = new DynamicKafkaRecordSerializationSchema(description);
        } else {
            inputFields = description["inputFields"].get<std::vector<std::string>>();
            inputTypes = description["inputTypes"].get<std::vector<std::string>>();
            recordSerializer = new DynamicKafkaRecordSerializationSchema(inputFields, inputTypes);
        }
    }

    if (initContext != nullptr && initContext->getRestoredCheckpointId().has_value()) {
        lastCheckpointId = initContext->getRestoredCheckpointId().value();
    }

    if (deliveryGuarantee == DeliveryGuarantee::EXACTLY_ONCE) {
        // 释放之前的事务，因为之前主流程未见到kafka开启事务相关操作，此处仅做关闭不做开启操作
        abortLingeringTransactions(states, lastCheckpointId);
    }

    if (upsertMode) {
        std::string error;
        if (kafkaProducerConfig->set("partitioner", "murmur2_random", error) != RdKafka::Conf::CONF_OK ||
            kafkaProducerConfig->set("max.in.flight.requests.per.connection", "1", error) != RdKafka::Conf::CONF_OK) {
            throw std::runtime_error("Cannot configure upsert-kafka producer: " + error);
        }
    }
    currentProducer1 =
        std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, std::to_string(producerIndexOne));
    if (upsertMode) {
        std::string error;
        rd_topic1 = RdKafka::Topic::create(currentProducer1->getKafkaProducer(), topic, nullptr, error);
        if (rd_topic1 == nullptr) {
            throw std::runtime_error("Cannot open upsert-kafka topic: " + error);
        }
        return;
    }
    currentProducer2 =
        std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, std::to_string(producerIndexTwo));
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    std::string errstr;
    this->kafkaProducerConfig->set("default_topic_conf", tconf, errstr);
    rd_topic1 = RdKafka::Topic::create(currentProducer1->getKafkaProducer(), topic, tconf, errstr);
    rd_topic2 = RdKafka::Topic::create(currentProducer2->getKafkaProducer(), topic, tconf, errstr);
    kafkaWriterState = new KafkaWriterState(transactionalIdPrefix);
    taskId = omnistream::TimerThreadPool::GetTimerThreadPoolInstance()->addPeriodicTask(
        5000, [](KafkaWriter* kafkaWriter) { kafkaWriter->timer_thread(); }, this);
}

KafkaWriter::~KafkaWriter()
{
    if (!upsertMode) {
        timer_worker_thread_flag.store(false);
        omnistream::TimerThreadPool::GetTimerThreadPoolInstance()->cancel(taskId);

        {
            std::lock_guard<std::mutex> lock(gMtx);
        }

        stop_flag.store(true);
        cv.notify_all();

        if (worker_thread.joinable()) {
            worker_thread.join();
        }
    }

    delete kafkaWriterState;
    kafkaWriterState = nullptr;
    delete recordSerializer;
    recordSerializer = nullptr;
    delete rd_topic1;
    rd_topic1 = nullptr;
    delete rd_topic2;
    rd_topic2 = nullptr;
}

void KafkaWriter::write(String* element)
{
    checkAsyncProducerException();
    if (unlikely(not binded)) {
        if (bindCore >= 0) {
            omnistream::BindCoreManager::GetInstance()->BindDirectCore(bindCore);
        }
        binded = true;
    }
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
    element->putRefCount();
}

void KafkaWriter::write(Row* element)
{
    checkAsyncProducerException();
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
}

void KafkaWriter::write(RowData* element)
{
    checkAsyncProducerException();
    auto record = recordSerializer->Serialize(element);
    ProduceRecord(record);
}

void KafkaWriter::write(omnistream::VectorBatch* input, int rowIndex)
{
    checkAsyncProducerException();
    if (upsertMode) {
        // A single producer preserves the order of updates to the same Kafka key.
        auto record = recordSerializer->serializeUpsert(input, rowIndex);
        auto* producer = currentProducer1->getKafkaProducer();
        auto status = producer->produce(
            rd_topic1,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY | RdKafka::Producer::RK_MSG_BLOCK,
            record.value ? record.value->data() : nullptr,
            record.value ? record.value->size() : 0,
            record.key.data(),
            record.key.size(),
            nullptr);
        if (status != RdKafka::ERR_NO_ERROR) {
            throw std::runtime_error("Upsert Kafka producer failed: " + RdKafka::err2str(status));
        }
        producer->poll(0);
        return;
    }
    auto record = recordSerializer->Serialize(input, rowIndex);
    ProduceRecord(record);
}

void KafkaWriter::Flush(bool endOfInput)
{
    if (deliveryGuarantee != DeliveryGuarantee::NONE || endOfInput) {
        if (upsertMode) {
            currentProducer1->Flush();
            return;
        }
        checkAsyncProducerException();
        {
            std::unique_lock<std::mutex> gLock(gMtx);
            handleRecord();
        }
        waitForPendingTasks();
        currentProducer1->Flush();
        currentProducer2->Flush();
    }
}

std::vector<KafkaCommittable> KafkaWriter::prepareCommit()
{
    if (deliveryGuarantee == DeliveryGuarantee::EXACTLY_ONCE) {
        auto committables = std::vector<KafkaCommittable>{
            KafkaCommittable::of(currentProducer1.get(), [this](FlinkKafkaInternalProducer* producer) {
                producerPool.push_back(static_cast<const std::shared_ptr<FlinkKafkaInternalProducer>>(producer));
            })};
        return committables;
    }
    return {};
}

void KafkaWriter::AbortCurrentProducer()
{
    if (currentProducer1->IsInTransaction()) {
        currentProducer1->AbortTransaction();
    }
}

void KafkaWriter::abortLingeringTransactions(
    const std::vector<KafkaWriterState>& recoveredStates, long startCheckpointId)
{
    auto prefixesToAbort = std::vector<std::string>{transactionalIdPrefix};

    if (!recoveredStates.empty()) {
        const auto& lastState = recoveredStates.front();
        if (lastState.getTransactionalIdPrefix() != transactionalIdPrefix) {
            prefixesToAbort.push_back(lastState.getTransactionalIdPrefix());
        }
    }

    TransactionAborter transactionAborter(
        [this](const std::string& transactionalId) { return getOrCreateTransactionalProducer(transactionalId); },
        [this](const std::shared_ptr<FlinkKafkaInternalProducer>& producer) { producerPool.push_back(producer); });
    transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
}

std::shared_ptr<FlinkKafkaInternalProducer> KafkaWriter::getTransactionalProducer(long checkpointId)
{
    std::shared_ptr<FlinkKafkaInternalProducer> producer;
    for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
        auto transactionalId = TransactionalIdFactory::buildTransactionalId(transactionalIdPrefix, 0, id);
        producer = getOrCreateTransactionalProducer(transactionalId);
    }
    lastCheckpointId = checkpointId;
    return producer;
}

std::shared_ptr<FlinkKafkaInternalProducer> KafkaWriter::getOrCreateTransactionalProducer(
    const std::string& transactionalId)
{
    auto producer = producerPool.empty() ? nullptr : producerPool.front();
    if (!producer) {
        producer = std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, transactionalId);
    } else {
        producerPool.pop_front();
    }
    return producer;
}

void KafkaWriter::ProduceRecord(KeyValueByteContainer& record)
{
    std::unique_lock<std::mutex> gLock(gMtx);
    values.push_back(record.value);
    valuesLens.push_back(record.valueLen);
    ++cur;
    if (cur >= limit) {
        handleRecord();
    }
}

void KafkaWriter::handleRecord()
{
    const size_t recordCount = values.size();
    if (recordCount == 0) {
        valuesLens.clear();
        cur = 0;
        return;
    }
    if (recordCount != valuesLens.size()) {
        INFO_RELEASE(
            "KafkaWriter::handleRecord cached record size mismatch, values size: "
            << recordCount << ", valuesLens size: " << valuesLens.size() << ", cur: " << cur);
        return;
    }

    const size_t mid = recordCount / 2;
    // 分割数据
    std::vector<char*> first_half(values.begin(), values.begin() + mid);
    std::vector<size_t> first_half_lens(valuesLens.begin(), valuesLens.begin() + mid);
    std::vector<char*> last_half(values.begin() + mid, values.end());
    std::vector<size_t> last_half_lens(valuesLens.begin() + mid, valuesLens.end());

    auto kafkaProducer1 = currentProducer1->getKafkaProducer();

    produce(kafkaProducer1, rd_topic1, first_half, first_half_lens);

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        tasks.emplace([this, last_half, last_half_lens]() {
            auto kafkaProducer2 = currentProducer2->getKafkaProducer();
            produce(kafkaProducer2, rd_topic2, last_half, last_half_lens);
        });
    }
    cv.notify_one();

    values.clear();
    valuesLens.clear();
    cur = 0;
}

void KafkaWriter::waitForPendingTasks()
{
    std::unique_lock<std::mutex> lock(queueMutex);
    tasksDrainedCv.wait(lock, [this]() { return tasks.empty() && inFlightTasks == 0; });
    if (asyncProducerException != nullptr) {
        INFO_RELEASE(
            "Error: KafkaWriter::waitForPendingTasks -> rethrow async producer exception "
            "asyncProducerException=present");
        std::rethrow_exception(asyncProducerException);
    }
}

void KafkaWriter::checkAsyncProducerException()
{
    std::lock_guard<std::mutex> lock(queueMutex);
    if (asyncProducerException != nullptr) {
        INFO_RELEASE(
            "Error: KafkaWriter::checkAsyncProducerException -> rethrow async producer exception "
            "asyncProducerException=present");
        std::rethrow_exception(asyncProducerException);
    }
}

void KafkaWriter::SetSubTaskIdx(int32_t subtaskIdx)
{
    this->instanceId = subtaskIdx;
    if (upsertMode) {
        return;
    }
    if (omnistream::BindCoreManager::GetInstance()->NeedBindSink()) {
        bindCore = omnistream::BindCoreManager::GetInstance()->GetSinkCore(subtaskIdx);
    }
    worker_thread = std::thread(&KafkaWriter::WorkerThreadFunc, this);
}

void KafkaWriter::produce(
    RdKafka::Producer* kafkaProducer,
    RdKafka::Topic* rd_topic,
    const std::vector<char*>& value,
    const std::vector<size_t>& valuesLen)
{
    const int32_t partitionCount = rd_topic->get_partition_num();
    const int32_t realPartition = partitionCount == 0 ? RdKafka::Topic::PARTITION_UA : (instanceId % partitionCount);
    RdKafka::ErrorCode resp = kafkaProducer->produce(
        rd_topic,
        realPartition,
        RdKafka::Producer::RK_MSG_FREE | RdKafka::Producer::RK_MSG_BLOCK,
        value,
        valuesLen,
        nullptr,
        0,
        nullptr);
    if (resp != RdKafka::ERR_NO_ERROR) {
        const std::string errorMessage =
            "Error: KafkaWriter::produce -> Kafka producer failed partition=" + std::to_string(realPartition) +
            " recordCount=" + std::to_string(value.size()) + " error=" + RdKafka::err2str(resp);
        INFO_RELEASE(errorMessage);
        throw std::runtime_error("Kafka producer failed");
    }

    kafkaProducer->poll(0);
}

std::vector<KafkaWriterState> KafkaWriter::snapshotState(long checkpointId)
{
    if (upsertMode) {
        NOT_IMPL_EXCEPTION;
    }
    Flush(true);
    if (deliveryGuarantee == DeliveryGuarantee::EXACTLY_ONCE) {
        auto currentProducer = getTransactionalProducer(checkpointId + 1);
        currentProducer->BeginTransaction();
    }
    return {*kafkaWriterState};
}
