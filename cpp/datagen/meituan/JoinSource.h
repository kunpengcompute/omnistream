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

#ifndef JOINSOURCE_H
#define JOINSOURCE_H

#include <map>
#include <vector>
#include <string>
#include <random>
#include <memory>
#include <atomic>
#include <mutex>
#include <nlohmann/json.hpp>
#include "OriginalRecord.h"
#include "functions/SourceContext.h"
#include "functions/AbstractRichFunction.h"
#include "functions/SourceFunction.h"
#include "functions/Configuration.h"
#include "table/data/vectorbatch/VectorBatch.h"

class JoinSource : public SourceFunction<omnistream::VectorBatch>, public AbstractRichFunction {
public:
    explicit JoinSource(const nlohmann::json& description);

    JoinSource(
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
        long sleepTime = 3000);

    std::unordered_map<long, std::vector<OriginalRecord*>>& getRecordsToCollect();
    void open(const Configuration& parameters) override;
    void run(SourceContext* ctx) override;
    void cancel() override;
    void generateRecordsForKey();

private:
    std::atomic<bool> running = true;
    int keysPerCheck = 100;
    int checkInterval = 1000;
    int minLeftRecordsPerKey = 1;
    int maxLeftRecordsPerKey = 1;
    int minRightRecordsPerKey = 0;
    int maxRightRecordsPerKey = 5;
    int recordValueSize = 10;
    int recordKeySize = 10;
    int leftMaxDelay = 10;
    int rightMaxDelay = 10000;
    long sleepTime = 3000;

    int currentSubtaskIndex = 0;
    long currentKeyId = 0;
    std::unordered_map<long, std::vector<OriginalRecord*>>* recordsToCollect;

    std::unique_ptr<std::mt19937> random;

    long getRandomLong();
    std::string getRandomAlphanumeric(int length);
    void originalRecordToBatch(OriginalRecord* record, omnistream::VectorBatch* batch, int index);
    omnistream::VectorBatch* createBatch(int size);
};

#endif // JOINSOURCE_H
