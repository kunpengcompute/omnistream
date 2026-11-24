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

#ifndef FLINK_TNEL_RANK_RANGE_H
#define FLINK_TNEL_RANK_RANGE_H

// Base class for RankRange.
class RankRange {
public:
    virtual ~RankRange() = default;
};

/// ---------------------------------------------------------------------
/// ConstantRankRange: A RankRange with fixed start and end values.
/// ---------------------------------------------------------------------
class ConstantRankRange : public RankRange {
public:
    ConstantRankRange(long rankStart, long rankEnd);

    // Getters for the constant rank range values.
    long getRankStart() const;
    long getRankEnd() const;

private:
    long rankStart_;
    long rankEnd_;
};

/// ---------------------------------------------------------------------
/// VariableRankRange: A RankRange where the rank end is determined at runtime.
/// ---------------------------------------------------------------------
class VariableRankRange : public RankRange {
public:
    explicit VariableRankRange(int rankEndIndex);

    // Get the index of the rank end field in the input data.
    int getRankEndIndex() const;

private:
    int rankEndIndex_;
};

#endif
