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

#ifndef KAFKARECORDSERIALIZATIONSCHEMA_H
#define KAFKARECORDSERIALIZATIONSCHEMA_H

#include <memory>
#include <vector>
#include <string>
#include "KeyValueByteContainer.h"

/**
 * A serialization schema which defines how to convert a value of type {@code T} to {@link
 * ProducerRecord}.
 *
 * @tparam T the type of values being serialized
 */
template <typename T>
class KafkaRecordSerializationSchema {
public:
    virtual ~KafkaRecordSerializationSchema() = default;

    /**
     * Serializes given element and returns it as a {@link ProducerRecord}.
     *
     * @param element element to be serialized
     * @param context context to possibly determine target partition
     * @param timestamp timestamp
     * @return Kafka {@link ProducerRecord}
     */
    virtual KeyValueByteContainer Serialize(T* element) = 0;

    /** Context providing information of the kafka record target location. */
    class KafkaSinkContext {
    public:
        /**
         * Get the ID of the subtask the KafkaSink is running on. The numbering starts from 0 and
         * goes up to parallelism-1. (parallelism as returned by {@link
         * #GetNumberOfParallelInstances()}
         *
         * @return ID of subtask
         */
        virtual int GetParallelInstanceId() const = 0;

        /** @return number of parallel KafkaSink tasks. */
        virtual int GetNumberOfParallelInstances() const = 0;

        /**
         * For a given topic id retrieve the available partitions.
         *
         * <p>After the first retrieval the returned partitions are cached. If the partitions are
         * updated the job has to be restarted to make the change visible.
         *
         * @param topic kafka topic with partitions
         * @return the ids of the currently available partitions
         */
        virtual std::vector<int> GetPartitionsForTopic(const std::string& topic) const = 0;
    };
};

#endif
