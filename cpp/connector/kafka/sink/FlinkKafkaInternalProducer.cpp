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

#include "FlinkKafkaInternalProducer.h"

FlinkKafkaInternalProducer::FlinkKafkaInternalProducer(RdKafka::Conf* properties, const std::string& transactionalId)
    : transactionalId_(transactionalId),
      inTransaction_(false),
      closed_(false)
{
    producer_ = RdKafka::Producer::create(properties, errstr);
    if (!producer_) {
        throw std::runtime_error("FAILED to create producer: " + errstr);
    }
}

FlinkKafkaInternalProducer::~FlinkKafkaInternalProducer()
{
    delete producer_;
}

RdKafka::Producer* FlinkKafkaInternalProducer::getKafkaProducer()
{
    return this->producer_;
}

void FlinkKafkaInternalProducer::Flush()
{
    const RdKafka::ErrorCode errorCode = producer_->flush(flushTimeout_);
    if (errorCode != RdKafka::ERR_NO_ERROR) {
        const int remainingMessages = producer_->outq_len();
        const std::string errorMessage =
            "Error: FlinkKafkaInternalProducer::Flush -> flush Kafka producer failed timeoutMs=" +
            std::to_string(flushTimeout_) + " remainingMessages=" + std::to_string(remainingMessages) +
            " error=" + RdKafka::err2str(errorCode);
        INFO_RELEASE(errorMessage);
        throw std::runtime_error("FAILED to flush Kafka producer");
    }
    if (inTransaction_) {
        FlushNewPartitions();
    }
}

void FlinkKafkaInternalProducer::BeginTransaction()
{
    RdKafka::Error* err = producer_->begin_transaction();
    if (err) {
        throw std::runtime_error("FAILED to begin transaction: " + err->str());
    }
    inTransaction_ = true;
}

void FlinkKafkaInternalProducer::AbortTransaction()
{
    LOG("AbortTransaction " + transactionalId_);
    if (!inTransaction_) {
        throw std::runtime_error("Transaction was not started");
    }
    RdKafka::Error* err = producer_->abort_transaction(timeout_);
    if (err) {
        throw std::runtime_error("FAILED to abort transaction: " + err->str());
    }
    inTransaction_ = false;
}

void FlinkKafkaInternalProducer::CommitTransaction()
{
    LOG("CommitTransaction " + transactionalId_);
    if (!inTransaction_) {
        throw std::runtime_error("Transaction was not started");
    }
    RdKafka::Error* err = producer_->commit_transaction(timeout_);
    if (err) {
        throw std::runtime_error("FAILED to Commit transaction: " + err->str());
    }
    inTransaction_ = false;
}

bool FlinkKafkaInternalProducer::IsInTransaction() const
{
    return inTransaction_;
}

void FlinkKafkaInternalProducer::Close()
{
    closed_ = true;
    if (inTransaction_) {
        producer_->poll(0);
    } else {
        producer_->poll(pollTimeout_);
    }
}

void FlinkKafkaInternalProducer::Close(int timeout)
{
    closed_ = true;
    producer_->poll(timeout);
}

bool FlinkKafkaInternalProducer::IsClosed() const
{
    return closed_;
}

const std::string& FlinkKafkaInternalProducer::getTransactionalId() const
{
    return transactionalId_;
}

int32_t FlinkKafkaInternalProducer::getEpoch() const
{
    // 此处java逻辑为反射
    return 0;
}

int64_t FlinkKafkaInternalProducer::getProducerId() const
{
    // 此处java逻辑为反射
    return 0;
}

void FlinkKafkaInternalProducer::initTransactionId(const std::string& transactionalId)
{
    if (transactionalId != transactionalId_) {
        setTransactionId(transactionalId);
        producer_->init_transactions(timeout_);
    }
}

void FlinkKafkaInternalProducer::setTransactionId(const std::string& transactionalId)
{
    if (transactionalId != transactionalId_) {
        if (inTransaction_) {
            throw std::runtime_error("Another transaction is still open.");
        }
        // 此处java逻辑为反射
    }
}

void FlinkKafkaInternalProducer::FlushNewPartitions()
{
    // 此处java逻辑为反射
}

void* FlinkKafkaInternalProducer::GetTransactionManager() const
{
    // 此处java逻辑为反射
    return nullptr;
}

void FlinkKafkaInternalProducer::transitionTransactionManagerStateTo(const std::string& state)
{
    // 此处java逻辑为反射
}

RdKafka::Conf* FlinkKafkaInternalProducer::withTransactionalId(
    RdKafka::Conf* properties, const std::string& transactionalId)
{
    if (transactionalId.empty()) {
        return properties;
    }
    // 将transactionalId存入properties，通过transactionalId创建producer
    std::string errStr;
    properties->set("transactional.id", transactionalId, errStr);
    return properties;
}

void FlinkKafkaInternalProducer::resumeTransaction(int64_t producerId, int32_t epoch)
{
    if (inTransaction_) {
        throw std::runtime_error("Already in transaction " + transactionalId_);
    }
    if (producerId < 0 || epoch < 0) {
        throw std::runtime_error(
            "Incorrect values for producerId " + std::to_string(producerId) + " and epoch " + std::to_string(epoch));
    }
    // 此处java逻辑为反射
}

void* FlinkKafkaInternalProducer::createProducerIdAndEpoch(int64_t producerId, int32_t epoch)
{
    // 此处java逻辑为反射
    return nullptr;
}
