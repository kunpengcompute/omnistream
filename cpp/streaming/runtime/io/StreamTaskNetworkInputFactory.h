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

#ifndef STREAMTASKNETWORKINPUTFACTORY_H
#define STREAMTASKNETWORKINPUTFACTORY_H

#include <memory>
#include "StreamTaskNetworkInput.h"
#include "typeutils/TypeSerializer.h"

namespace omnistream::datastream {
class StreamTaskNetworkInputFactory {
public:
    static std::unique_ptr<StreamTaskNetworkInput> create(
        TypeSerializer* inputSerializer, std::vector<long>& channelInfos);
};
} // namespace omnistream::datastream

#endif
