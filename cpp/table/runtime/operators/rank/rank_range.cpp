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

#include "rank_range.h"

// --- ConstantRankRange Implementation ---

ConstantRankRange::ConstantRankRange(int64_t rankStart, int64_t rankEnd) : rankStart_(rankStart), rankEnd_(rankEnd)
{
}

int64_t ConstantRankRange::getRankStart() const
{
    return rankStart_;
}

int64_t ConstantRankRange::getRankEnd() const
{
    return rankEnd_;
}

// --- VariableRankRange Implementation ---

VariableRankRange::VariableRankRange(int rankEndIndex) : rankEndIndex_(rankEndIndex)
{
}

int VariableRankRange::getRankEndIndex() const
{
    return rankEndIndex_;
}
