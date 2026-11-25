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

#ifndef FLINK_TNEL_OPERATORCONSTANTS_H
#define FLINK_TNEL_OPERATORCONSTANTS_H
#include <string_view>
namespace omnistream {
namespace datastream {
constexpr std::string_view OPERATOR_NAME_MAP = "org.apache.flink.streaming.api.operators.StreamMap";
constexpr std::string_view OPERATOR_NAME_REDUCE =
    "org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator";
constexpr std::string_view OPERATOR_NAME_FILTER = "org.apache.flink.streaming.api.operators.StreamFilter";
constexpr std::string_view OPERATOR_NAME_ADDSOURCE = "org.apache.flink.streaming.api.operators.StreamSource";
constexpr std::string_view OPERATOR_NAME_FROMSOURCE = "org.apache.flink.streaming.api.operators.SourceOperator";
constexpr std::string_view OPERATOR_NAME_SINK_WRITER =
    "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator";
constexpr std::string_view OPERATOR_NAME_COMMIT_OPERATOR =
    "org.apache.flink.streaming.runtime.operators.sink.CommitterOperator";
constexpr std::string_view OPERATOR_NAME_FLATMAP = "org.apache.flink.streaming.api.operators.StreamFlatMap";
constexpr std::string_view OPERATOR_NAME_GROUP_AGG = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
} // namespace datastream
} // namespace omnistream

#endif
