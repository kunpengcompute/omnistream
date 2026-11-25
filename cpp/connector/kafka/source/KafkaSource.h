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

#ifndef FLINK_TNEL_KAFKASOURCE_H
#define FLINK_TNEL_KAFKASOURCE_H

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <unordered_map>
#include <future>
#include "connector/kafka/source/split/KafkaPartitionSplitSerializer.h"
#include "connector/kafka/source/reader/KafkaSourceReader.h"
#include "ConsumerConfigUtil.h"
#include "connector/kafka/utils/ConfigLoader.h"

class KafkaSource {
public:
    explicit KafkaSource(nlohmann::json& opDescriptionJSON, bool isBatch);
    ~KafkaSource()
    {
    }
    SourceReader<KafkaPartitionSplit>* createReader(SourceReaderContext* readerContext) const;
    KafkaPartitionSplitSerializer* getSplitSerializer() const;

private:
    bool isBatch;
    std::unordered_map<std::string, std::string> props;
    KafkaRecordDeserializationSchema* deserializationSchema;
};

#endif // FLINK_TNEL_KAFKASOURCE_H
