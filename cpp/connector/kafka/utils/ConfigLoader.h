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

#ifndef FLINK_TNEL_CONFIGLOADER_H
#define FLINK_TNEL_CONFIGLOADER_H

#include <unordered_map>
#include <string>

class ConfigLoader {
public:
    static std::unordered_map<std::string, std::string> LoadKafkaConfig(const std::string& confFile);

    static std::unordered_map<std::string, std::string> LoadKafkaConsumerConfig();

    static std::unordered_map<std::string, std::string> LoadKafkaProducerConfig();

    static std::unordered_map<std::string, std::string>& GetKafkaConsumerConfig();

    static std::unordered_map<std::string, std::string>& GetKafkaProducerConfig();

    static std::unordered_map<std::string, std::string> g_consumer_config;

    static std::unordered_map<std::string, std::string> g_producer_config;
};

#endif // FLINK_TNEL_CONFIGLOADER_H
