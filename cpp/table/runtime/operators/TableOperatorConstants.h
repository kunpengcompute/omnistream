/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_TABLEOPERATORCONSTANTS_H
#define FLINK_TNEL_TABLEOPERATORCONSTANTS_H
#include <string_view>
namespace omnistream {
constexpr std::string_view OPERATOR_NAME_STREAM_CALC = "StreamExecCalc";
constexpr std::string_view OPERATOR_NAME_STREAM_EXPAND = "StreamExecExpand";
constexpr std::string_view OPERATOR_NAME_KEYED_PROCESS_OPERATOR =
    "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
constexpr std::string_view OPERATOR_NAME_STREAM_JOIN =
    "org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator";
// Flink StreamExecJoin instantiates this for SEMI (EXISTS) / ANTI (NOT EXISTS) join.
constexpr std::string_view OPERATOR_NAME_STREAM_SEMI_ANTI_JOIN =
    "org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator";
constexpr std::string_view OPERATOR_NAME_WATERMARK_ASSIGNER =
    "org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator";
constexpr std::string_view OPERATOR_NAME_WINDOW_INNER_JOIN =
    "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.InnerJoinOperator";
constexpr std::string_view OPERATOR_NAME_WINDOW_LEFT_OUTER_JOIN =
    "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.LeftOuterJoinOperator";
constexpr std::string_view OPERATOR_NAME_WINDOW_RIGHT_OUTER_JOIN =
    "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.RightOuterJoinOperator";
constexpr std::string_view OPERATOR_NAME_PROCESS_OPERATOR = "org.apache.flink.streaming.api.operators.ProcessOperator";
constexpr std::string_view OPERATOR_NAME_COLLECT_SINK =
    "org.apache.flink.streaming.api.operators.collect.CollectSinkOperator";
constexpr std::string_view OPERATOR_NAME_SINK = "org.apache.flink.table.runtime.operators.sink.SinkOperator";
constexpr std::string_view OPERATOR_NAME_STREAM_SINK = "org.apache.flink.streaming.api.operators.StreamSink";
constexpr std::string_view OPERATOR_NAME_STREAM_SOURCE = "org.apache.flink.streaming.api.operators.StreamSource";
constexpr std::string_view OPERATOR_NAME_SOURCE_OPERATOR = "org.apache.flink.streaming.api.operators.SourceOperator";
constexpr std::string_view OPERATOR_NAME_LOCAL_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
constexpr std::string_view OPERATOR_NAME_GLOBAL_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
constexpr std::string_view OPERATOR_NAME_STREAMRECORDTIMESTAMPINSERTER =
    "org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter";
constexpr std::string_view OPERATOR_NAME_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
constexpr std::string_view OPERATOR_NAME_CONSTRAINTENFORCER =
    "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer";
constexpr std::string_view OPERATOR_NAME_GROUP_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
// Flink StreamExecWindowTableFunction instantiates WindowTableFunctionOperator for standalone Window TVF.
constexpr std::string_view OPERATOR_NAME_WINDOW_TABLE_FUNCTION =
    "org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator";
constexpr std::string_view OPERATOR_NAME_INPUT_CONVERSION =
    "org.apache.flink.table.runtime.operators.source.InputConversionOperator";
constexpr std::string_view OPERATOR_NAME_FILTER = "org.apache.flink.streaming.api.operators.StreamFilter";
constexpr std::string_view OPERATOR_NAME_SINK_WRITER =
    "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator";
constexpr std::string_view OPERATOR_NAME_COMMIT_OPERATOR =
    "org.apache.flink.streaming.runtime.operators.sink.CommitterOperator";
constexpr std::string_view OPERATOR_NAME_OUTPUT_CONVERSION =
    "org.apache.flink.table.runtime.operators.sink.OutputConversionOperator";
constexpr std::string_view OPERATOR_NAME_STREAMING_FILE_WRITER =
    "org.apache.flink.connector.file.table.stream.StreamingFileWriter";
constexpr std::string_view OPERATOR_NAME_PARTITION_COMMITTER =
    "org.apache.flink.connector.file.table.stream.PartitionCommitter";
constexpr std::string_view OPERATOR_NAME_KEYED_CO_PROCESS =
    "org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator";
} // namespace omnistream
#endif // FLINK_TNEL_TABLEOPERATORCONSTANTS_H
