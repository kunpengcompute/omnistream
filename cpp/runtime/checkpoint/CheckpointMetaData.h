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

#ifndef FLINK_TNEL_CHECKPOINTMETADATA_H
#define FLINK_TNEL_CHECKPOINTMETADATA_H
#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
/** Encapsulates all the meta data for a checkpoint. */
class CheckpointMetaData {
public:
    CheckpointMetaData(int64_t checkpointId, int64_t timestamp)
        : CheckpointMetaData(
              checkpointId,
              timestamp,
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                  .count())
    {
    }

    CheckpointMetaData(int64_t checkpointId, int64_t timestamp, int64_t receiveTimestamp)
        : checkpointId(checkpointId),
          timestamp(timestamp),
          receiveTimestamp(receiveTimestamp)
    {
    }

    int64_t GetCheckpointId() const
    {
        return checkpointId;
    }

    int64_t GetTimestamp() const
    {
        return timestamp;
    }

    int64_t GetReceiveTimestamp() const
    {
        return receiveTimestamp;
    }

    bool operator==(const CheckpointMetaData& other) const
    {
        return checkpointId == other.checkpointId && timestamp == other.timestamp &&
               receiveTimestamp == other.receiveTimestamp;
    }

    bool operator!=(const CheckpointMetaData& other) const
    {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream& os, const CheckpointMetaData& meta)
    {
        os << "CheckpointMetaData{"
           << "checkpointId=" << meta.checkpointId << ", receiveTimestamp=" << meta.receiveTimestamp
           << ", timestamp=" << meta.timestamp << '}';
        return os;
    }

    std::string ToString() const
    {
        nlohmann::json j;
        j["stateHandleName"] = "CheckpointMetaData"; // For consistency
        j["checkpointId"] = checkpointId;
        j["timestamp"] = timestamp;
        j["receiveTimestamp"] = receiveTimestamp;

        return j.dump();
    }

private:
    /** The ID of the checkpoint. */
    int64_t checkpointId;
    /** The timestamp of the checkpoint triggering. */
    int64_t timestamp;
    /** The timestamp of the checkpoint receiving by this subtask. */
    int64_t receiveTimestamp;
};

namespace std {
template <>
struct hash<CheckpointMetaData> {
    size_t operator()(const CheckpointMetaData& meta) const
    {
        int hashMultiplier = 31;
        int bitShift = 32;
        int result = static_cast<int>(
            static_cast<uint64_t>(meta.GetCheckpointId()) ^
            (static_cast<uint64_t>(meta.GetCheckpointId()) >> bitShift));
        result = result + static_cast<int>(
                              static_cast<uint64_t>(meta.GetTimestamp()) ^
                              (static_cast<uint64_t>(meta.GetTimestamp()) >> bitShift));
        result = hashMultiplier * result;
        result = result + static_cast<int>(
                              static_cast<uint64_t>(meta.GetReceiveTimestamp()) ^
                              (static_cast<uint64_t>(meta.GetReceiveTimestamp()) >> bitShift));
        result = hashMultiplier * result;
        return static_cast<size_t>(result);
    }
};
} // namespace std
#endif // FLINK_TNEL_CHECKPOINTMETADATA_H
