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

#include <string>

enum class RestoreSavepointMode {
    OMNI_INTERNAL,
    FLINK_COMPATIBLE
};

enum class FlinkSavepointAdaptorType {
    None,
    OmniIsCompatible,
    StreamingJoinNoUniqueKeyAdaptor,
    StreamingLeftOuterJoinNoUniqueKeyAdaptor,
    GroupAggAdaptor
};

inline const char* flinkSavepointAdaptorTypeName(FlinkSavepointAdaptorType type) noexcept
{
    switch (type) {
        case FlinkSavepointAdaptorType::None: return "None";
        case FlinkSavepointAdaptorType::OmniIsCompatible: return "OmniIsCompatible";
        case FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor: return "StreamingJoinNoUniqueKeyAdaptor";
        case FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor:
            return "StreamingLeftOuterJoinNoUniqueKeyAdaptor";
        default: return "Unknown";
    }
}

struct FlinkSavepointAdaptorInfo {
    FlinkSavepointAdaptorType type = FlinkSavepointAdaptorType::None;
    std::string reason;
};
