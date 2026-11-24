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

#ifndef STREAMTONEINPUTPROCESSOR_H
#define STREAMTONEINPUTPROCESSOR_H

#include <nlohmann/json.hpp>
#include "streaming/runtime/io/StreamTaskNetworkOutput.h"
#include "streaming/runtime/io/StreamTaskNetworkInput.h"

using json = nlohmann::json;

namespace omnistream::datastream {
class StreamOneInputProcessor {
public:
    StreamOneInputProcessor(
        std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput,
        std::unique_ptr<StreamTaskNetworkInput> streamTaskInput);
    ~StreamOneInputProcessor();

    /* data */
    std::unique_ptr<StreamTaskNetworkOutput> streamTaskOutput;
    std::unique_ptr<StreamTaskNetworkInput> streamTaskInput;

    int processInput(const uint8_t* input_buffer, size_t input_size, long channelInfo, int32_t& inputNumber);
};

} // namespace omnistream::datastream
#endif
