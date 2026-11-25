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

#include "GeneratorConfig.h"
std::vector<GeneratorConfig> GeneratorConfig::split(int n) const
{
    std::vector<GeneratorConfig> results;
    if (n == 1) {
        // No split required.
        results.push_back(*this);
    } else {
        if (n == 0) {
            GErrorLog("split num is 0");
            throw std::runtime_error("split num is 0");
        }

        int64_t subMaxEvents = maxEvents / n;
        int64_t subFirstEventId = firstEventId;
        for (int i = 0; i < n; i++) {
            if (i == n - 1) {
                // Don't loose any events to round-down.
                subMaxEvents = maxEvents - subMaxEvents * (n - 1);
            }
            results.push_back(copyWithConfig(subFirstEventId, subMaxEvents, firstEventNumber));
            subFirstEventId += subMaxEvents;
        }
    }
    return results;
}
/** Return an estimate of the bytes needed by {@code numEvents}. */
int64_t GeneratorConfig::estimatedBytesForEvents(int64_t numEvents) const
{
    int64_t numPersons = (numEvents * personProportion) / totalProportion;
    int64_t numAuctions = (numEvents * auctionProportion) / totalProportion;
    int64_t numBids = (numEvents * bidProportion) / totalProportion;
    return numPersons * configuration.avgPersonByteSize + numAuctions * configuration.avgAuctionByteSize +
           numBids * configuration.avgBidByteSize;
}
