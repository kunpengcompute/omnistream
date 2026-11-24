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

#include "typeconstants.h"

const char* TYPE_NAME_VOID = "Void";
const char* TYPE_NAME_LONG = "Long";

const char* TYPE_NAME_INT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.IntSerializer";
const char* TYPE_NAME_BIGINT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.BigIntSerializer";
const char* TYPE_NAME_LONG_SERIALIZER = "org.apache.flink.api.common.typeutils.base.LongSerializer";
const char* TYPE_NAME_DOUBLE_SERIALIZER = "org.apache.flink.api.common.typeutils.base.DoubleSerializer";
const char* TYPE_NAME_TUPLE_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.TupleSerializer";
const char* TYPE_NAME_POJO_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.PojoSerializer";
const char* TYPE_NAME_MAP_SERIALIZER = "org.apache.flink.api.common.typeutils.base.MapSerializer";
const char* TYPE_NAME_LIST_SERIALIZER = "org.apache.flink.api.common.typeutils.base.ListSerializer";
const char* TYPE_NAME_VOID_SERIALIZER = "org.apache.flink.api.common.typeutils.base.VoidSerializer";

// basic
const char* TYPE_NAME_STRING = "String";
const char* TYPE_NAME_STRING_CLASS = "java.lang.String";
const char* TYPE_NAME_STRING_CLASS_LINE = "java_lang_String";
const char* TYPE_NAME_STRING_SERIALIZER = "org.apache.flink.api.common.typeutils.base.StringSerializer";

// other
const char* TYPE_NAME_VOID_NAMESPACE = "VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_CLASS = "org.apache.flink.runtime.state.VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_CLASS_LINE = "org_apache_flink_runtime_state_VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_SERIALIZER = "org.apache.flink.runtime.state.VoidNamespaceSerializer";

const char* TYPE_NAME_ROW_DATA = "RowData";
const char* TYPE_NAME_ROW_DATA_CLASS = "org.apache.flink.table.data.RowData";
const char* TYPE_NAME_ROW_DATA_CLASS_LINE = "org_apache_flink_table_data_RowData";
const char* TYPE_NAME_ROW_DATA_SERIALIZER = "org.apache.flink.table.runtime.typeutils.RowDataSerializer";

const char* TYPE_NAME_BINARY_ROW_DATA = "BinaryRowData";
const char* TYPE_NAME_BINARY_ROW_DATA_CLASS = "org.apache.flink.table.data.binary.BinaryRowData";
const char* TYPE_NAME_BINARY_ROW_DATA_CLASS_LINE = "org_apache_flink_table_data_binary_BinaryRowData";
const char* TYPE_NAME_BINARY_ROW_DATA_SERIALIZER = "org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer";

const char* TYPE_NAME_TIMER = "TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_CLASS = "org.apache.flink.streaming.api.operators.TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_CLASS_LINE = "org_apache_flink_streaming_api_operators_TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_SERIALIZER = "org.apache.flink.streaming.api.operators.TimerSerializer";

const char* TYPE_NAME_BYTE_PRIMITIVE_ARRAY_SERIALIZER =
    "org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer";

const char* TYPE_NAME_JOIN_TUPLE = "JoinTuple";
const char* TYPE_NAME_JOIN_TUPLE_CLASS =
    "com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.JoinTuple";
const char* TYPE_NAME_JOIN_TUPLE_CLASS_LINE =
    "com_huawei_omniruntime_flink_runtime_api_state_serializer_model_entity_JoinTuple";

const char* TYPE_NAME_JOIN_TUPLE2 = "JoinTuple2";
const char* TYPE_NAME_JOIN_TUPLE2_CLASS =
    "com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.JoinTuple2";
const char* TYPE_NAME_JOIN_TUPLE2_CLASS_LINE =
    "com_huawei_omniruntime_flink_runtime_api_state_serializer_model_entity_JoinTuple2";

const char* TYPE_NAME_XXH128_HASH = "XxH128_hash";
const char* TYPE_NAME_XXH128_HASH_CLASS =
    "com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.XxHash128";
const char* TYPE_NAME_XXH128_HASH_CLASS_LINE =
    "com_huawei_omniruntime_flink_runtime_api_state_serializer_model_entity_XxHash128";

const char* TYPE_NAME_VECTOR_BATCH = "VectorBatch";
const char* TYPE_NAME_VECTOR_BATCH_CLASS =
    "com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.VectorBatch";
const char* TYPE_NAME_VECTOR_BATCH_CLASS_LINE =
    "com_huawei_omniruntime_flink_runtime_api_state_serializer_model_entity_VectorBatch";

const char* TYPE_NAME_SORTED_VECTOR_LONG = "SortedVectorLong";
const char* TYPE_NAME_SORTED_VECTOR_LONG_CLASS =
    "com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.SortedVectorLong";
const char* TYPE_NAME_SORTED_VECTOR_LONG_CLASS_LINE =
    "com_huawei_omniruntime_flink_runtime_api_state_serializer_model_entity_SortedVectorLong";

const char* TYPE_NAME_TIME_WINDOW = "TimeWindow";
const char* TYPE_NAME_TIME_WINDOW_CLASS = "org.apache.flink.table.runtime.operators.window.TimeWindow";
const char* TYPE_NAME_TIME_WINDOW_CLASS_LINE = "org_apache_flink_table_runtime_operators_window_TimeWindow";
const char* TYPE_NAME_TIME_WINDOW_SERIALIZER = "org.apache.flink.table.runtime.operators.window.TimeWindow$Serializer";
const char* TYPE_NAME_EXTERNAL_SERIALIZER = "org.apache.flink.table.runtime.typeutils.ExternalSerializer";
