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

#include "rank_range.h"

// --- ConstantRankRange Implementation ---

ConstantRankRange::ConstantRankRange(long rankStart, long rankEnd) : rankStart_(rankStart), rankEnd_(rankEnd)
{
}

long ConstantRankRange::getRankStart() const
{
    return rankStart_;
}

long ConstantRankRange::getRankEnd() const
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
