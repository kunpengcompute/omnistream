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

#ifndef JOBINFORMATIONPOD_H
#define JOBINFORMATIONPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "JobIDPOD.h"

namespace omnistream {

class JobInformationPOD {
public:
    static constexpr const char* RECOVERY_FORMAT_OMNI_INTERNAL = "OMNI_INTERNAL";
    static constexpr const char* RECOVERY_FORMAT_FLINK_COMPATIBLE = "FLINK_COMPATIBLE";

    JobInformationPOD() = default;
    JobInformationPOD(const JobIDPOD& jobId, const std::string& jobName)
        : jobId(jobId),
          jobName(jobName),
          autoWatermarkInterval(0),
          recoverySavepointFormat(RECOVERY_FORMAT_OMNI_INTERNAL)
    {
    }
    JobInformationPOD(const JobIDPOD& jobId, const std::string& jobName, long autoWatermarkInterval)
        : jobId(jobId),
          jobName(jobName),
          autoWatermarkInterval(autoWatermarkInterval),
          recoverySavepointFormat(RECOVERY_FORMAT_OMNI_INTERNAL)
    {
    }

    JobInformationPOD(const JobInformationPOD& other)
        : jobId(other.jobId),
          jobName(other.jobName),
          autoWatermarkInterval(other.autoWatermarkInterval),
          recoverySavepointFormat(other.recoverySavepointFormat)
    {
    }

    JobInformationPOD(JobInformationPOD&& other) noexcept
        : jobId(std::move(other.jobId)),
          jobName(std::move(other.jobName)),
          autoWatermarkInterval(other.autoWatermarkInterval),
          recoverySavepointFormat(std::move(other.recoverySavepointFormat))
    {
    }

    bool operator==(const JobInformationPOD& other) const
    {
        return jobId == other.jobId && jobName == other.jobName;
    }

    JobInformationPOD& operator=(const JobInformationPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        jobId = other.jobId;
        jobName = other.jobName;
        autoWatermarkInterval = other.autoWatermarkInterval;
        recoverySavepointFormat = other.recoverySavepointFormat;
        return *this;
    }

    JobInformationPOD& operator=(JobInformationPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        jobId = std::move(other.jobId);
        jobName = std::move(other.jobName);
        autoWatermarkInterval = other.autoWatermarkInterval;
        recoverySavepointFormat = std::move(other.recoverySavepointFormat);
        return *this;
    }

    JobIDPOD getJobId() const
    {
        return jobId;
    }
    void setJobId(const JobIDPOD& jobId_)
    {
        this->jobId = jobId_;
    }

    std::string getJobName() const
    {
        return jobName;
    }
    void setJobName(const std::string& jobName_)
    {
        this->jobName = jobName_;
    }

    long getAutoWatermarkInterval() const
    {
        return autoWatermarkInterval;
    }

    void setAutoWatermarkInterval(const long autoWatermarkInterval_)
    {
        this->autoWatermarkInterval = autoWatermarkInterval_;
    }

    std::string getRecoverySavepointFormat() const
    {
        return recoverySavepointFormat;
    }

    void setRecoverySavepointFormat(const std::string& recoverySavepointFormat_)
    {
        this->recoverySavepointFormat = recoverySavepointFormat_;
    }

    bool isCompatibleRecoverySavepointFormat() const
    {
        return recoverySavepointFormat == RECOVERY_FORMAT_FLINK_COMPATIBLE;
    }

    void checkRecoverySavepointFormat() const
    {
        if (recoverySavepointFormat == RECOVERY_FORMAT_FLINK_COMPATIBLE ||
            recoverySavepointFormat == RECOVERY_FORMAT_OMNI_INTERNAL)
            return;

        INFO_RELEASE("Unknown RecoverySavepointFormat: " + recoverySavepointFormat);
        throw std::runtime_error("Unknown RecoverySavepointFormat: " + recoverySavepointFormat);
    }

    std::string toString() const
    {
        return "JobInformationPOD{ jobId=" + jobId.toString() + ", jobName='" + jobName + '\'' +
               ", autoWatermarkInterval='" + std::to_string(autoWatermarkInterval) + '\'' +
               ", recoverySavepointFormat='" + recoverySavepointFormat + '\'' + '}';
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(JobInformationPOD, jobId, jobName, autoWatermarkInterval, recoverySavepointFormat)

private:
    JobIDPOD jobId;
    std::string jobName;
    long autoWatermarkInterval;
    std::string recoverySavepointFormat;
};

} // namespace omnistream
namespace std {
template <>
struct hash<omnistream::JobInformationPOD> {
    std::size_t operator()(const omnistream::JobInformationPOD& obj) const
    {
        std::size_t h1 = hash_value(obj.getJobId());
        std::size_t h2 = std::hash<std::string>{}(obj.getJobName());
        return h1 ^ (h2 << 1);
    }
};
} // namespace std

#endif // JOBINFORMATIONPOD_H
