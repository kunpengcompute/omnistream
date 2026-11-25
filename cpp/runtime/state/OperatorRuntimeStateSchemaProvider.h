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

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "core/typeutils/TypeSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"

namespace omnistream {
class OperatorRuntimeStateSchemaProvider {
public:
    struct MapStateSchema {
        BackendDataType keyBackendType;
        BackendDataType valueBackendType;
    };

    virtual ~OperatorRuntimeStateSchemaProvider() = default;

    virtual std::optional<MapStateSchema> resolveMapStateSchema(
        const std::string& stateName,
        BackendDataType checkpointKeyBackendType,
        BackendDataType checkpointValueBackendType) const = 0;
};

class StreamingJoinRuntimeStateSchemaProvider final : public OperatorRuntimeStateSchemaProvider {
public:
    explicit StreamingJoinRuntimeStateSchemaProvider(bool leftOuterJoin) : leftOuterJoin_(leftOuterJoin)
    {
    }

    std::optional<MapStateSchema> resolveMapStateSchema(
        const std::string& stateName,
        BackendDataType checkpointKeyBackendType,
        BackendDataType checkpointValueBackendType) const override
    {
        std::optional<MapStateSchema> runtimeSchema;
        if (stateName == StreamingJoinSavepointUtil::LEFT_STATE_NAME) {
            runtimeSchema = MapStateSchema{
                BackendDataType::SHARED_ROW_BK,
                leftOuterJoin_ ? BackendDataType::TUPLE_INT32_INT32 : BackendDataType::INT_BK};
        } else if (stateName == StreamingJoinSavepointUtil::RIGHT_STATE_NAME) {
            runtimeSchema = MapStateSchema{BackendDataType::SHARED_ROW_BK, BackendDataType::INT_BK};
        }
        if (!runtimeSchema.has_value()) {
            return std::nullopt;
        }

        const bool compatibleCheckpointKey = checkpointKeyBackendType == BackendDataType::ROW_BK ||
                                             checkpointKeyBackendType == BackendDataType::SHARED_ROW_BK;
        if (!compatibleCheckpointKey || checkpointValueBackendType != runtimeSchema->valueBackendType) {
            throw std::runtime_error("StreamingJoin runtime MapState schema mismatch for state=" + stateName);
        }
        return runtimeSchema;
    }

private:
    bool leftOuterJoin_;
};

class OperatorRuntimeStateSchemaProviderFactory {
public:
    OperatorRuntimeStateSchemaProviderFactory() = delete;

    static std::unique_ptr<OperatorRuntimeStateSchemaProvider> create(const nlohmann::json& operatorDescription)
    {
        // left/right input specs distinguish StreamingJoin from WindowJoin and
        // other operators whose descriptions may also contain joinType.
        if (!operatorDescription.is_object() ||
            !operatorDescription.contains(StreamingJoinSavepointUtil::JOIN_TYPE_FIELD) ||
            !operatorDescription.at(StreamingJoinSavepointUtil::JOIN_TYPE_FIELD).is_string() ||
            !operatorDescription.contains(StreamingJoinSavepointUtil::LEFT_INPUT_SPEC_FIELD) ||
            !operatorDescription.at(StreamingJoinSavepointUtil::LEFT_INPUT_SPEC_FIELD).is_string() ||
            !operatorDescription.contains(StreamingJoinSavepointUtil::RIGHT_INPUT_SPEC_FIELD) ||
            !operatorDescription.at(StreamingJoinSavepointUtil::RIGHT_INPUT_SPEC_FIELD).is_string()) {
            return nullptr;
        }

        const std::string joinType =
            operatorDescription.at(StreamingJoinSavepointUtil::JOIN_TYPE_FIELD).get<std::string>();
        if (joinType == StreamingJoinSavepointUtil::INNER_JOIN_TYPE) {
            return std::make_unique<StreamingJoinRuntimeStateSchemaProvider>(false);
        }
        if (joinType == StreamingJoinSavepointUtil::LEFT_OUTER_JOIN_TYPE) {
            return std::make_unique<StreamingJoinRuntimeStateSchemaProvider>(true);
        }
        return nullptr;
    }
};

} // namespace omnistream
