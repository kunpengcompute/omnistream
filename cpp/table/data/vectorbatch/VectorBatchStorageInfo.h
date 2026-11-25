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

#include <cstdint>
namespace omnistream {
/**
 * 63             48 47                         16 15              0
 * +----------------+-----------------------------+----------------+
 * | keyGroup: 16bit| sequenceNumber: 32bit       | rowId: 16bit   |
 * +----------------+-----------------------------+----------------+
 */
using ComboId = uint64_t;
using VectorBatchId = uint64_t;
static constexpr ComboId INVALID_COMBO_ID = UINT64_MAX;
static constexpr VectorBatchId INVALID_VECTOR_BATCH_ID = UINT64_MAX;
} // namespace omnistream
