/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Mulan PSL v2.
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
