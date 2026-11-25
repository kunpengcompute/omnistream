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

#pragma once

#include <optional>
#include <stdexcept>

namespace omnistream {
enum class InputFormat {
    FLINK_ROW,
    OMNI_BATCH
};

// Published before restore/invoke. Channel replacement must not change the wire format.
class InputFormatState {
public:
    void initialize(InputFormat format)
    {
        if (format_.has_value()) {
            throw std::logic_error("Input format is already initialized");
        }
        format_ = format;
    }

    InputFormat get() const
    {
        if (!format_.has_value()) {
            throw std::logic_error("Input format must be initialized before consuming input");
        }
        return *format_;
    }

private:
    std::optional<InputFormat> format_;
};
} // namespace omnistream
