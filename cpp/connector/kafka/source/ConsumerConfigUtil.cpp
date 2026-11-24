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

#include "ConsumerConfigUtil.h"

namespace omnistream {
std::map<std::string, std::string> ConsumerConfig{
    {"fetch.max.wait.ms", "fetch.wait.max.ms"},
    {"max.poll.records", "max.poll.records"},
    {"key.deserializer", ""},
    {"value.deserializer", ""},
    {"group.id", "group.id"},
    {"group.instance.id", "group.instance.id"},
    {"max.poll.interval.ms", "max.poll.interval.ms"},
    {"session.timeout.ms", "session.timeout.ms"},
    {"heartbeat.interval.ms", "heartbeat.interval.ms"},
    {"bootstrap.servers", "bootstrap.servers"},
    {"client.dns.lookup", "client.dns.lookup"},
    {"enable.auto.commit", "enable.auto.commit"},
    {"auto.commit.interval.ms", "auto.commit.interval.ms"},
    {"partition.assignment.strategy", "partition.assignment.strategy"},
    {"auto.offset.reset", "auto.offset.reset"},
    {"fetch.min.bytes", "fetch.min.bytes"},
    {"fetch.max.bytes", "fetch.max.bytes"},
    {"metadata.max.age.ms", "metadata.max.age.ms"},
    {"max.partition.fetch.bytes", "max.partition.fetch.bytes"},
    {"send.buffer.bytes", "socket.send.buffer.bytes"},
    {"receive.buffer.bytes", "socket.receive.buffer.bytes"},
    {"client.id", "client.id"},
    {"client.rack", "client.rack"},
    {"reconnect.backoff.ms", "reconnect.backoff.ms"},
    {"reconnect.backoff.max.ms", "reconnect.backoff.max.ms"},
    {"retry.backoff.ms", "retry.backoff.ms"},
    {"check.crcs", "check.crcs"},
    {"socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.ms"},
    {"connections.max.idle.ms", "connections.max.idle.ms"},
    {"allow.auto.create.topics", "allow.auto.create.topics"},
    {"security.protocol", "security.protocol"},
    {"sasl.mechanism", "sasl.mechanism"},
    {"commit.offsets.on.checkpoint", "commit.offsets.on.checkpoint"}};
}

const std::map<std::string, std::string>& ConsumerConfigUtil::GetConfig()
{
    return omnistream::ConsumerConfig;
}
