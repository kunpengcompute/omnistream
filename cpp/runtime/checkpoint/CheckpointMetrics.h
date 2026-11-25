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

#ifndef OMNIRUNTIME_CHECKPOINTMETRICS_H
#define OMNIRUNTIME_CHECKPOINTMETRICS_H
#include <cstdint>
#include <ostream>
#include <cassert>
#include <nlohmann/json.hpp>

class CheckpointMetrics {
public:
    static constexpr int64_t UNSET = -1L;

    CheckpointMetrics()
        : bytesProcessedDuringAlignment(UNSET),
          bytesPersistedDuringAlignment(UNSET),
          alignmentDurationNanos(UNSET),
          syncDurationMillis(UNSET),
          asyncDurationMillis(UNSET),
          checkpointStartDelayNanos(UNSET),
          unalignedCheckpoint(false),
          bytesPersistedOfThisCheckpoint(0),
          totalBytesPersisted(0)
    {
    }

    CheckpointMetrics(
        int64_t bytesProcessedDuringAlignment,
        int64_t bytesPersistedDuringAlignment,
        int64_t alignmentDurationNanos,
        int64_t syncDurationMillis,
        int64_t asyncDurationMillis,
        int64_t checkpointStartDelayNanos,
        bool unalignedCheckpoint,
        int64_t bytesPersistedOfThisCheckpoint,
        int64_t totalBytesPersisted)
        : bytesProcessedDuringAlignment(bytesProcessedDuringAlignment),
          bytesPersistedDuringAlignment(bytesPersistedDuringAlignment),
          alignmentDurationNanos(alignmentDurationNanos),
          syncDurationMillis(syncDurationMillis),
          asyncDurationMillis(asyncDurationMillis),
          checkpointStartDelayNanos(checkpointStartDelayNanos),
          unalignedCheckpoint(unalignedCheckpoint),
          bytesPersistedOfThisCheckpoint(bytesPersistedOfThisCheckpoint),
          totalBytesPersisted(totalBytesPersisted)
    {
        if (!(bytesProcessedDuringAlignment >= -1)) {
            THROW_LOGIC_EXCEPTION("bytesProcessedDuringAlignment not valid");
        }
        if (!(bytesPersistedDuringAlignment >= -1)) {
            THROW_LOGIC_EXCEPTION("bytesPersistedDuringAlignment not valid");
        }
        if (!(syncDurationMillis >= -1)) {
            THROW_LOGIC_EXCEPTION("syncDurationMillis not valid");
        }
        if (!(asyncDurationMillis >= -1)) {
            THROW_LOGIC_EXCEPTION("asyncDurationMillis not valid");
        }
        if (!(alignmentDurationNanos >= -1)) {
            THROW_LOGIC_EXCEPTION("alignmentDurationNanos not valid");
        }
        if (!(checkpointStartDelayNanos >= -1)) {
            THROW_LOGIC_EXCEPTION("checkpointStartDelayNanos not valid");
        }
        if (!(bytesPersistedOfThisCheckpoint >= 0)) {
            THROW_LOGIC_EXCEPTION("bytesPersistedOfThisCheckpoint not valid");
        }
        if (!(totalBytesPersisted >= 0)) {
            THROW_LOGIC_EXCEPTION("totalBytesPersisted not valid");
        }
    }

    int64_t getBytesProcessedDuringAlignment() const
    {
        return bytesProcessedDuringAlignment;
    }

    int64_t getBytesPersistedDuringAlignment() const
    {
        return bytesPersistedDuringAlignment;
    }

    int64_t getAlignmentDurationNanos() const
    {
        return alignmentDurationNanos;
    }

    int64_t getSyncDurationMillis() const
    {
        return syncDurationMillis;
    }

    int64_t getAsyncDurationMillis() const
    {
        return asyncDurationMillis;
    }

    int64_t getCheckpointStartDelayNanos() const
    {
        return checkpointStartDelayNanos;
    }

    bool getUnalignedCheckpoint() const
    {
        return unalignedCheckpoint;
    }

    int64_t getBytesPersistedOfThisCheckpoint() const
    {
        return bytesPersistedOfThisCheckpoint;
    }

    int64_t getTotalBytesPersisted() const
    {
        return totalBytesPersisted;
    }

    bool operator==(const CheckpointMetrics& other) const
    {
        return bytesProcessedDuringAlignment == other.bytesProcessedDuringAlignment &&
               bytesPersistedDuringAlignment == other.bytesPersistedDuringAlignment &&
               alignmentDurationNanos == other.alignmentDurationNanos &&
               syncDurationMillis == other.syncDurationMillis && asyncDurationMillis == other.asyncDurationMillis &&
               checkpointStartDelayNanos == other.checkpointStartDelayNanos &&
               unalignedCheckpoint == other.unalignedCheckpoint &&
               bytesPersistedOfThisCheckpoint == other.bytesPersistedOfThisCheckpoint &&
               totalBytesPersisted == other.totalBytesPersisted;
    }

    bool operator!=(const CheckpointMetrics& other) const
    {
        return !(*this == other);
    }

    std::string ToString() const
    {
        nlohmann::json j;
        j["stateHandleName"] = "CheckpointMetrics"; // For consistency
        j["bytesProcessedDuringAlignment"] = bytesProcessedDuringAlignment;
        j["bytesPersistedDuringAlignment"] = bytesPersistedDuringAlignment;
        j["alignmentDurationNanos"] = alignmentDurationNanos;
        j["syncDurationMillis"] = syncDurationMillis;
        j["asyncDurationMillis"] = asyncDurationMillis;
        j["checkpointStartDelayNanos"] = checkpointStartDelayNanos;
        j["unalignedCheckpoint"] = unalignedCheckpoint;
        j["bytesPersistedOfThisCheckpoint"] = bytesPersistedOfThisCheckpoint;
        j["totalBytesPersisted"] = totalBytesPersisted;

        return j.dump();
    }

private:
    int64_t bytesProcessedDuringAlignment;
    int64_t bytesPersistedDuringAlignment;
    int64_t alignmentDurationNanos;
    int64_t syncDurationMillis;
    int64_t asyncDurationMillis;
    int64_t checkpointStartDelayNanos;
    bool unalignedCheckpoint;
    int64_t bytesPersistedOfThisCheckpoint;
    int64_t totalBytesPersisted;
};
#endif // OMNIRUNTIME_CHECKPOINTMETRICS_H
