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

#ifndef NETTY_BUFFER_CONF_H
#define NETTY_BUFFER_CONF_H

namespace omnistream {

class NettyBufferConf {
public:
    int totalPoolSize;
    int bufferSize;
    int configuredBufferPerChannel;
    int numOfFloatingBufferPerGate;

    NettyBufferConf(
        int totalPoolSize = 1000,
        int bufferSize = 32 * 1024,
        int numRequiredBuffersPerPool = 2,
        int maxBuffersPerPool = 8)
        : totalPoolSize(totalPoolSize),
          bufferSize(bufferSize),
          configuredBufferPerChannel(numRequiredBuffersPerPool),
          numOfFloatingBufferPerGate(maxBuffersPerPool)
    {
    }
};

} // namespace omnistream

#endif // NETTY_BUFFER_CONF_H
