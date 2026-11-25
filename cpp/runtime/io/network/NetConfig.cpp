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

#include "NetConfig.h"

namespace omnistream {

const std::string NetConfig::SERVER_THREAD_GROUP_NAME = "Flink Net Server";
const std::string NetConfig::CLIENT_THREAD_GROUP_NAME = "Flink Net Client";

NetConfig::NetConfig() : serverAddress(""), serverPort(0)
{
}

NetConfig::NetConfig(const std::string& serverAddress, int serverPort)
    : serverAddress(serverAddress),
      serverPort(serverPort)
{
}

NetConfig::~NetConfig()
{
}

std::string NetConfig::getServerAddress() const
{
    return serverAddress;
}

void NetConfig::setServerAddress(const std::string& serverAddress_)
{
    this->serverAddress = serverAddress_;
}

int NetConfig::getServerPort() const
{
    return serverPort;
}

void NetConfig::setServerPort(int serverPort_)
{
    this->serverPort = serverPort_;
}

std::string NetConfig::toString() const
{
    std::stringstream ss;
    ss << "NetConfig{"
       << "serverAddress='" << serverAddress << '\'' << ", serverPort=" << serverPort << '}';
    return ss.str();
}

} // namespace omnistream
