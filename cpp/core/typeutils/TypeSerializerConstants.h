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

#ifndef OMNISTREAM_TYPESERIALIZERCONSTANTS_H
#define OMNISTREAM_TYPESERIALIZERCONSTANTS_H
#include <string_view>
constexpr std::string_view TYPE_SERIALIZER_TUPLE = "org.apache.flink.api.java.typeutils.runtime.TupleSerializer";
constexpr std::string_view TYPE_SERIALIZER_STRING = "org.apache.flink.api.common.typeutils.base.StringSerializer";
constexpr std::string_view TYPE_SERIALIZER_LONG = "org.apache.flink.api.common.typeutils.base.LongSerializer";
#endif
