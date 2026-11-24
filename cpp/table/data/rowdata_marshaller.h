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

#ifndef OMNISTREAM_ROWDATA_MARSHALLER_H
#define OMNISTREAM_ROWDATA_MARSHALLER_H
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/type/string_ref.h"
using namespace omniruntime;

using VBToRowSerializer = void (*)(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* result, int32_t pos);

using RowToVBDeSerializer = void (*)(vec::BaseVector* baseVector, int32_t rowIdx, BinaryRowData* input, int32_t pos);

extern std::vector<VBToRowSerializer> rowSerializerCenter;
extern std::vector<RowToVBDeSerializer> rowDeserializerCenter;

#endif // OMNISTREAM_ROWDATA_MARSHALLER_H
