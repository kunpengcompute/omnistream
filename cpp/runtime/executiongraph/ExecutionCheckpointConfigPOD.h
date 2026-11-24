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

#ifndef OMNISTREAM_EXECUTIONCHECKPOINTCONFIGPOD_H
#define OMNISTREAM_EXECUTIONCHECKPOINTCONFIGPOD_H

#include <functional>
#include <string>
#include <sstream>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace omnistream {

class ExecutionCheckpointConfigPOD {
public:
    ExecutionCheckpointConfigPOD() = default;

    ExecutionCheckpointConfigPOD(
        std::int64_t alignedCheckpointTimeoutSecond,
        std::int64_t alignedCheckpointTimeoutNano,
        std::int64_t checkpointIdOfIgnoredInFlightData,
        std::int64_t checkpointInterval,
        std::int64_t checkpointTimeout,
        std::string checkpointingMode,
        std::string externalizedCheckpointCleanup,
        bool forceUnalignedCheckpoints,
        int maxConcurrentCheckpoints,
        std::int64_t minPauseBetweenCheckpoints,
        int tolerableCheckpointFailureNumber,
        bool unalignedCheckpointsEnabled,
        bool checkpointAfterTasksFinishEnabled)
        : alignedCheckpointTimeoutSecond(alignedCheckpointTimeoutSecond),
          alignedCheckpointTimeoutNano(alignedCheckpointTimeoutNano),
          checkpointIdOfIgnoredInFlightData(checkpointIdOfIgnoredInFlightData),
          checkpointInterval(checkpointInterval),
          checkpointTimeout(checkpointTimeout),
          checkpointingMode(checkpointingMode),
          externalizedCheckpointCleanup(externalizedCheckpointCleanup),
          forceUnalignedCheckpoints(forceUnalignedCheckpoints),
          maxConcurrentCheckpoints(maxConcurrentCheckpoints),
          minPauseBetweenCheckpoints(minPauseBetweenCheckpoints),
          tolerableCheckpointFailureNumber(tolerableCheckpointFailureNumber),
          unalignedCheckpointsEnabled(unalignedCheckpointsEnabled),
          checkpointAfterTasksFinishEnabled(checkpointAfterTasksFinishEnabled)
    {
    }

    std::int64_t getAlignedCheckpointTimeoutSecond() const
    {
        return alignedCheckpointTimeoutSecond;
    }

    void setAlignedCheckpointTimeoutSecond(std::int64_t input)
    {
        alignedCheckpointTimeoutSecond = input;
    }

    std::int64_t getAlignedCheckpointTimeoutNano() const
    {
        return alignedCheckpointTimeoutNano;
    }

    void setAlignedCheckpointTimeoutNano(std::int64_t input)
    {
        alignedCheckpointTimeoutNano = input;
    }

    std::int64_t getCheckpointIdOfIgnoredInFlightData() const
    {
        return checkpointIdOfIgnoredInFlightData;
    }

    void setCheckpointIdOfIgnoredInFlightData(std::int64_t input)
    {
        checkpointIdOfIgnoredInFlightData = input;
    }

    std::int64_t getCheckpointInterval() const
    {
        return checkpointInterval;
    }

    void setCheckpointInterval(std::int64_t input)
    {
        checkpointInterval = input;
    }

    std::int64_t getCheckpointTimeout() const
    {
        return checkpointTimeout;
    }

    void setCheckpointTimeout(std::int64_t input)
    {
        checkpointTimeout = input;
    }

    std::string getCheckpointingMode() const
    {
        return checkpointingMode;
    }

    void setCheckpointingMode(const std::string& input)
    {
        checkpointingMode = input;
    }

    std::string getExternalizedCheckpointCleanup() const
    {
        return externalizedCheckpointCleanup;
    }

    void setExternalizedCheckpointCleanup(const std::string& input)
    {
        externalizedCheckpointCleanup = input;
    }

    bool getForceUnalignedCheckpoints() const
    {
        return forceUnalignedCheckpoints;
    }

    void setForceUnalignedCheckpoints(bool input)
    {
        forceUnalignedCheckpoints = input;
    }

    int getMaxConcurrentCheckpoints() const
    {
        return maxConcurrentCheckpoints;
    }

    void setMaxConcurrentCheckpoints(int input)
    {
        maxConcurrentCheckpoints = input;
    }

    std::int64_t getMinPauseBetweenCheckpoints() const
    {
        return minPauseBetweenCheckpoints;
    }

    void setMinPauseBetweenCheckpoints(std::int64_t input)
    {
        minPauseBetweenCheckpoints = input;
    }

    int getTolerableCheckpointFailureNumber() const
    {
        return tolerableCheckpointFailureNumber;
    }

    void setTolerableCheckpointFailureNumber(int input)
    {
        tolerableCheckpointFailureNumber = input;
    }

    bool getUnalignedCheckpointsEnabled() const
    {
        return unalignedCheckpointsEnabled;
    }

    void setUnalignedCheckpointsEnabled(bool input)
    {
        unalignedCheckpointsEnabled = input;
    }

    bool getCheckpointAfterTasksFinishEnabled() const
    {
        return checkpointAfterTasksFinishEnabled;
    }

    void setCheckpointAfterTasksFinishEnabled(bool input)
    {
        checkpointAfterTasksFinishEnabled = input;
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "ExecutionCheckpointConfigPOD{"
            << "alignedCheckpointTimeoutSecond=" << alignedCheckpointTimeoutSecond
            << ", alignedCheckpointTimeoutNano=" << alignedCheckpointTimeoutNano
            << ", checkpointIdOfIgnoredInFlightData=" << checkpointIdOfIgnoredInFlightData
            << ", checkpointInterval=" << checkpointInterval << ", checkpointTimeout=" << checkpointTimeout
            << ", checkpointingMode=" << checkpointingMode
            << ", externalizedCheckpointCleanup=" << externalizedCheckpointCleanup
            << ", forceUnalignedCheckpoints=" << forceUnalignedCheckpoints
            << ", maxConcurrentCheckpoints=" << maxConcurrentCheckpoints
            << ", minPauseBetweenCheckpoints=" << minPauseBetweenCheckpoints
            << ", tolerableCheckpointFailureNumber=" << tolerableCheckpointFailureNumber
            << ", unalignedCheckpointsEnabled=" << unalignedCheckpointsEnabled
            << ", checkpointAfterTasksFinishEnabled=" << checkpointAfterTasksFinishEnabled << '}';
        return oss.str();
    }

    bool operator==(const ExecutionCheckpointConfigPOD& other) const
    {
        return alignedCheckpointTimeoutSecond == other.alignedCheckpointTimeoutSecond &&
               alignedCheckpointTimeoutNano == other.alignedCheckpointTimeoutNano &&
               checkpointIdOfIgnoredInFlightData == other.checkpointIdOfIgnoredInFlightData &&
               checkpointInterval == other.checkpointInterval && checkpointTimeout == other.checkpointTimeout &&
               checkpointingMode == other.checkpointingMode &&
               externalizedCheckpointCleanup == other.externalizedCheckpointCleanup &&
               forceUnalignedCheckpoints == other.forceUnalignedCheckpoints &&
               maxConcurrentCheckpoints == other.maxConcurrentCheckpoints &&
               minPauseBetweenCheckpoints == other.minPauseBetweenCheckpoints &&
               tolerableCheckpointFailureNumber == other.tolerableCheckpointFailureNumber &&
               unalignedCheckpointsEnabled == other.unalignedCheckpointsEnabled &&
               checkpointAfterTasksFinishEnabled == other.checkpointAfterTasksFinishEnabled;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        ExecutionCheckpointConfigPOD,
        alignedCheckpointTimeoutSecond,
        alignedCheckpointTimeoutNano,
        checkpointIdOfIgnoredInFlightData,
        checkpointInterval,
        checkpointTimeout,
        checkpointingMode,
        externalizedCheckpointCleanup,
        forceUnalignedCheckpoints,
        maxConcurrentCheckpoints,
        minPauseBetweenCheckpoints,
        tolerableCheckpointFailureNumber,
        unalignedCheckpointsEnabled,
        checkpointAfterTasksFinishEnabled)

private:
    std::int64_t alignedCheckpointTimeoutSecond = 0;
    std::int64_t alignedCheckpointTimeoutNano = 0;
    std::int64_t checkpointIdOfIgnoredInFlightData = -1;
    std::int64_t checkpointInterval = -1;
    std::int64_t checkpointTimeout = 600000;
    std::string checkpointingMode = "EXACTLY_ONCE";
    std::string externalizedCheckpointCleanup = "NO_EXTERNALIZED_CHECKPOINTS";
    bool forceUnalignedCheckpoints = false;
    int maxConcurrentCheckpoints = 1;
    std::int64_t minPauseBetweenCheckpoints = 0;
    int tolerableCheckpointFailureNumber = -1;
    bool unalignedCheckpointsEnabled = true;
    bool checkpointAfterTasksFinishEnabled = false;
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::ExecutionCheckpointConfigPOD> {
    size_t operator()(const omnistream::ExecutionCheckpointConfigPOD& obj) const
    {
        size_t h1 = std::hash<std::int64_t>()(obj.getAlignedCheckpointTimeoutSecond());
        size_t h2 = std::hash<std::int64_t>()(obj.getAlignedCheckpointTimeoutNano());
        size_t h3 = std::hash<std::int64_t>()(obj.getCheckpointIdOfIgnoredInFlightData());
        size_t h4 = std::hash<std::int64_t>()(obj.getCheckpointInterval());
        size_t h5 = std::hash<std::int64_t>()(obj.getCheckpointTimeout());
        size_t h6 = std::hash<std::string>()(obj.getCheckpointingMode());
        size_t h7 = std::hash<std::string>()(obj.getExternalizedCheckpointCleanup());
        size_t h8 = std::hash<bool>()(obj.getForceUnalignedCheckpoints());
        size_t h9 = std::hash<int>()(obj.getMaxConcurrentCheckpoints());
        size_t h10 = std::hash<std::int64_t>()(obj.getMinPauseBetweenCheckpoints());
        size_t h11 = std::hash<int>()(obj.getTolerableCheckpointFailureNumber());
        size_t h12 = std::hash<bool>()(obj.getUnalignedCheckpointsEnabled());
        size_t h13 = std::hash<bool>()(obj.getCheckpointAfterTasksFinishEnabled());

        size_t seed = 0;
        const size_t C = 0x7f4a7c15;

        seed ^= (h1 + C + (seed << 6) + (seed >> 2));
        seed ^= (h2 + C + (seed << 6) + (seed >> 2));
        seed ^= (h3 + C + (seed << 6) + (seed >> 2));
        seed ^= (h4 + C + (seed << 6) + (seed >> 2));
        seed ^= (h5 + C + (seed << 6) + (seed >> 2));
        seed ^= (h6 + C + (seed << 6) + (seed >> 2));
        seed ^= (h7 + C + (seed << 6) + (seed >> 2));
        seed ^= (h8 + C + (seed << 6) + (seed >> 2));
        seed ^= (h9 + C + (seed << 6) + (seed >> 2));
        seed ^= (h10 + C + (seed << 6) + (seed >> 2));
        seed ^= (h11 + C + (seed << 6) + (seed >> 2));
        seed ^= (h12 + C + (seed << 6) + (seed >> 2));
        seed ^= (h13 + C + (seed << 6) + (seed >> 2));

        return seed;
    }
};
} // namespace std

#endif // OMNISTREAM_EXECUTIONCHECKPOINTCONFIGPOD_H
