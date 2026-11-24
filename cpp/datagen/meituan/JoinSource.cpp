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

#include "JoinSource.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <random>
#include <climits>

long JoinSource::getRandomLong()
{
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    std::uniform_int_distribution<long> dist(LONG_MIN, LONG_MAX);
    return dist(gen);
}

std::string JoinSource::getRandomAlphanumeric(int length)
{
    static const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dist(0, chars.size() - 1);

    std::string result;
    result.reserve(length);
    for (int i = 0; i < length; ++i) {
        result += chars[dist(gen)];
    }
    return result;
}

JoinSource::JoinSource(
    int keysPerCheck,
    int checkInterval,
    int minLeftRecordsPerKey,
    int maxLeftRecordsPerKey,
    int minRightRecordsPerKey,
    int maxRightRecordsPerKey,
    int recordValueSize,
    int recordKeySize,
    int leftMaxDelay,
    int rightMaxDelay,
    long sleepTime)
    : running(true),
      keysPerCheck(keysPerCheck),
      checkInterval(checkInterval),
      minLeftRecordsPerKey(minLeftRecordsPerKey),
      maxLeftRecordsPerKey(maxLeftRecordsPerKey),
      minRightRecordsPerKey(minRightRecordsPerKey),
      maxRightRecordsPerKey(maxRightRecordsPerKey),
      recordValueSize(recordValueSize),
      recordKeySize(recordKeySize),
      leftMaxDelay(leftMaxDelay),
      rightMaxDelay(rightMaxDelay),
      sleepTime(sleepTime),
      currentSubtaskIndex(0),
      currentKeyId(0)
{
}

JoinSource::JoinSource(const nlohmann::json& configuration)
{
    if (configuration.contains("configMap") && !configuration["configMap"].is_null()) {
        auto configMap = configuration["configMap"];
        if (configMap.contains("checkInterval")) {
            checkInterval = configMap["checkInterval"];
        }
        if (configMap.contains("minLeftRecordsPerKey")) {
            minLeftRecordsPerKey = configMap["minLeftRecordsPerKey"];
        }
        if (configMap.contains("maxLeftRecordsPerKey")) {
            maxLeftRecordsPerKey = configMap["maxLeftRecordsPerKey"];
        }
        if (configMap.contains("minRightRecordsPerKey")) {
            minRightRecordsPerKey = configMap["minRightRecordsPerKey"];
        }
        if (configMap.contains("maxRightRecordsPerKey")) {
            maxRightRecordsPerKey = configMap["maxRightRecordsPerKey"];
        }
        if (configMap.contains("keysPerCheck")) {
            keysPerCheck = configMap["keysPerCheck"];
        }
        if (configMap.contains("rightMaxDelay")) {
            rightMaxDelay = configMap["rightMaxDelay"];
        }
        if (configMap.contains("leftMaxDelay")) {
            leftMaxDelay = configMap["leftMaxDelay"];
        }
        if (configMap.contains("recordKeySize")) {
            recordKeySize = configMap["recordKeySize"];
        }
        if (configMap.contains("recordValueSize")) {
            recordValueSize = configMap["recordValueSize"];
        }
        if (configMap.contains("sleepTime")) {
            sleepTime = configMap["sleepTime"];
        }
    }
}
void JoinSource::open(const Configuration& parameters)
{
    AbstractRichFunction::open(parameters);
    currentSubtaskIndex = this->getRuntimeContext()->getIndexOfThisSubtask();
    recordsToCollect = new std::unordered_map<long, std::vector<OriginalRecord*>>();
}

void JoinSource::run(SourceContext* ctx)
{
    auto startTime = std::chrono::steady_clock::now();

    while (running) {
        auto loopStartTime = std::chrono::steady_clock::now();
        {
            ctx->getCheckpointLock()->mutex.lock();
            for (int i = 0; i < keysPerCheck; i++) {
                generateRecordsForKey();
            }

            std::unordered_map<std::string, std::pair<omnistream::VectorBatch*, omnistream::VectorBatch*>>
                batchesToCollect;

            auto currentTimestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count();

            for (auto& entry : *recordsToCollect) {
                for (auto& record : entry.second) {
                    if (record->getTimestamp() <= currentTimestamp) {
                        // Create a new pair for key if not exist
                        auto key = record->getKey();
                        if (batchesToCollect.find(key) == batchesToCollect.end()) {
                            batchesToCollect[key] = std::make_pair(
                                createBatch(record->getLeftTotalCount()), createBatch(record->getRightTotalCount()));
                        }

                        // Add record into batch
                        if (record->isLeft()) {
                            originalRecordToBatch(record, batchesToCollect[key].first, record->getCurrentLeftId() - 1);
                        } else {
                            originalRecordToBatch(
                                record, batchesToCollect[key].second, record->getCurrentRightId() - 1);
                        }
                    }
                }

                entry.second.erase(
                    std::remove_if(
                        entry.second.begin(),
                        entry.second.end(),
                        [currentTimestamp](const OriginalRecord* record) {
                            return record->getTimestamp() <= currentTimestamp;
                        }),
                    entry.second.end());
            }

            // Collect Batches
            for (auto& entry : batchesToCollect) {
                ctx->collect(entry.second.first);
                ctx->collect(entry.second.second);
            }
            for (auto it = recordsToCollect->begin(); it != recordsToCollect->end();) {
                if (it->second.empty()) {
                    it = recordsToCollect->erase(it);
                } else {
                    ++it;
                }
            }
            auto elapsed = std::chrono::steady_clock::now() - loopStartTime;
            auto millisToSleep = checkInterval - std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (millisToSleep > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(millisToSleep));
            }
            ctx->getCheckpointLock()->mutex.unlock();
        }

        auto elapsedTime = std::chrono::steady_clock::now() - startTime;
        if (sleepTime > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(elapsedTime).count() >= sleepTime) {
            cancel();
        }
    }
}

void JoinSource::cancel()
{
    running = false;
}

void JoinSource::generateRecordsForKey()
{
    std::string key = std::to_string(currentSubtaskIndex) + "_" + std::to_string(currentKeyId);
    if (static_cast<int>(key.size()) < recordKeySize) {
        key += ("_" + getRandomAlphanumeric(recordKeySize - key.size()));
    }

    long leftRecords =
        minLeftRecordsPerKey + std::abs(getRandomLong()) % (maxLeftRecordsPerKey - minLeftRecordsPerKey + 1);
    long rightRecords =
        minRightRecordsPerKey + std::abs(getRandomLong()) % (maxRightRecordsPerKey - minRightRecordsPerKey + 1);
    std::vector<OriginalRecord*> records;
    long baseTimestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();

    for (long i = 0; i < leftRecords; ++i) {
        OriginalRecord* record = new OriginalRecord();
        record->setKey(key);
        record->setLeft(true);
        record->setLeftTotalCount(leftRecords);
        record->setRightTotalCount(rightRecords);
        record->setValue(getRandomAlphanumeric(recordValueSize));
        record->setTimestamp(baseTimestamp + std::abs(getRandomLong()) % leftMaxDelay);
        records.push_back(record);
    }

    for (long i = 0; i < rightRecords; ++i) {
        OriginalRecord* record = new OriginalRecord();
        record->setKey(key);
        record->setLeft(false);
        record->setLeftTotalCount(leftRecords);
        record->setRightTotalCount(rightRecords);
        record->setValue(getRandomAlphanumeric(recordValueSize));
        record->setTimestamp(baseTimestamp + std::abs(getRandomLong()) % rightMaxDelay);
        records.push_back(record);
    }

    std::sort(records.begin(), records.end(), [](OriginalRecord* a, OriginalRecord* b) { return *a < *b; });
    long currentLeftId = 1;
    long currentRightId = 1;

    for (auto& record : records) {
        if (record->isLeft()) {
            record->setCurrentLeftId(currentLeftId);
            currentLeftId++;
        } else {
            record->setCurrentRightId(currentRightId);
            currentRightId++;
        }
    }

    recordsToCollect->emplace(currentKeyId, records);
    currentKeyId++;
    if (currentKeyId == LONG_MAX) {
        currentKeyId = 0;
    }
}

std::unordered_map<long, std::vector<OriginalRecord*>>& JoinSource::getRecordsToCollect()
{
    return *recordsToCollect;
}

void JoinSource::originalRecordToBatch(OriginalRecord* record, omnistream::VectorBatch* batch, int index)
{
    std::string_view key = record->getKey();
    std::string_view value = record->getValue();
    static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(0))
        ->SetValue(index, key);
    static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(1))
        ->SetValue(index, value);
    batch->setTimestamp(index, record->getTimestamp());
    batch->setRowKind(index, RowKind::INSERT);
}

omnistream::VectorBatch* JoinSource::createBatch(int size)
{
    omnistream::VectorBatch* batch = new omnistream::VectorBatch(size);
    batch->Append(new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(size));
    batch->Append(new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(size));
    return batch;
}
