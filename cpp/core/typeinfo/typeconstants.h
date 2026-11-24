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

#ifndef FLINK_TNEL_TYPECONSTANTS_H
#define FLINK_TNEL_TYPECONSTANTS_H

extern const char* TYPE_NAME_VOID;
extern const char* TYPE_NAME_LONG;
extern const char* TYPE_NAME_LIST;

extern const char* TYPE_NAME_INT_SERIALIZER;
extern const char* TYPE_NAME_BIGINT_SERIALIZER;
extern const char* TYPE_NAME_LONG_SERIALIZER;
extern const char* TYPE_NAME_DOUBLE_SERIALIZER;
extern const char* TYPE_NAME_TUPLE_SERIALIZER;
extern const char* TYPE_NAME_POJO_SERIALIZER;
extern const char* TYPE_NAME_MAP_SERIALIZER;
extern const char* TYPE_NAME_LIST_SERIALIZER;
extern const char* TYPE_NAME_VOID_SERIALIZER;

// basic
extern const char* TYPE_NAME_STRING;
extern const char* TYPE_NAME_STRING_CLASS;
extern const char* TYPE_NAME_STRING_CLASS_LINE;
extern const char* TYPE_NAME_STRING_SERIALIZER;

// other
extern const char* TYPE_NAME_VOID_NAMESPACE;
extern const char* TYPE_NAME_VOID_NAMESPACE_CLASS;
extern const char* TYPE_NAME_VOID_NAMESPACE_CLASS_LINE;
extern const char* TYPE_NAME_VOID_NAMESPACE_SERIALIZER;

extern const char* TYPE_NAME_ROW_DATA;
extern const char* TYPE_NAME_ROW_DATA_CLASS;
extern const char* TYPE_NAME_ROW_DATA_CLASS_LINE;
extern const char* TYPE_NAME_ROW_DATA_SERIALIZER;

extern const char* TYPE_NAME_BINARY_ROW_DATA;
extern const char* TYPE_NAME_BINARY_ROW_DATA_CLASS;
extern const char* TYPE_NAME_BINARY_ROW_DATA_CLASS_LINE;
extern const char* TYPE_NAME_BINARY_ROW_DATA_SERIALIZER;

extern const char* TYPE_NAME_TIMER;
extern const char* TYPE_NAME_TIMER_CLASS;
extern const char* TYPE_NAME_TIMER_CLASS_LINE;
extern const char* TYPE_NAME_TIMER_SERIALIZER;

extern const char* TYPE_NAME_BYTE_PRIMITIVE_ARRAY_SERIALIZER;

extern const char* TYPE_NAME_JOIN_TUPLE;
extern const char* TYPE_NAME_JOIN_TUPLE_CLASS;
extern const char* TYPE_NAME_JOIN_TUPLE_CLASS_LINE;

extern const char* TYPE_NAME_JOIN_TUPLE2;
extern const char* TYPE_NAME_JOIN_TUPLE2_CLASS;
extern const char* TYPE_NAME_JOIN_TUPLE2_CLASS_LINE;

extern const char* TYPE_NAME_XXH128_HASH;
extern const char* TYPE_NAME_XXH128_HASH_CLASS;
extern const char* TYPE_NAME_XXH128_HASH_CLASS_LINE;

extern const char* TYPE_NAME_VECTOR_BATCH;
extern const char* TYPE_NAME_VECTOR_BATCH_CLASS;
extern const char* TYPE_NAME_VECTOR_BATCH_CLASS_LINE;

extern const char* TYPE_NAME_SORTED_VECTOR_LONG;
extern const char* TYPE_NAME_SORTED_VECTOR_LONG_CLASS;
extern const char* TYPE_NAME_SORTED_VECTOR_LONG_CLASS_LINE;

extern const char* TYPE_NAME_TIME_WINDOW;
extern const char* TYPE_NAME_TIME_WINDOW_CLASS;
extern const char* TYPE_NAME_TIME_WINDOW_CLASS_LINE;
extern const char* TYPE_NAME_TIME_WINDOW_SERIALIZER;

extern const char* TYPE_NAME_EXTERNAL_SERIALIZER;

#endif // FLINK_TNEL_TYPECONSTANTS_H
