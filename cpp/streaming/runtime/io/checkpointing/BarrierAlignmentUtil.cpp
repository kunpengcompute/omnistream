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

#include "BarrierAlignmentUtil.h"
namespace omnistream::runtime {
long BarrierAlignmentUtil::getTimerDelay(long clockMillis, const CheckpointBarrier& announcedBarrier)
{
    long alignedCheckpointTimeout = announcedBarrier.GetCheckpointOptions()->GetAlignedCheckpointTimeout();
    long timePassedSinceCheckpointStart = clockMillis - announcedBarrier.GetTimestamp();
    long retVal = alignedCheckpointTimeout - timePassedSinceCheckpointStart;
    return retVal > 0 ? retVal : 0;
}
} // namespace omnistream::runtime
