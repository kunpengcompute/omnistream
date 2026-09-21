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

#ifndef OMNISTREAM_STREAMING_FILE_SINK_H
#define OMNISTREAM_STREAMING_FILE_SINK_H

#include "./Buckets.h"
#include "./BucketWriter.h"
#include "./BulkWriter.h"
#include "./OutputFileConfig.h"
#include <memory>
#include "core/fs/Path.h"
#include "formats/csv/CsvRowEncoder.h"
#include <array>
#include <random>

// Strip a "file:" URI scheme from a sink path, e.g. "file:///tmp/out" -> "/tmp/out",
// so part files land under the configured directory instead of a relative one.
inline std::string stripFileUriScheme(std::string p)
{
    if (p.rfind("file:", 0) == 0) {
        p = p.substr(5);
        while (!p.empty() && p[0] == '/') {
            p.erase(p.begin());
        }
        if (p.empty() || p[0] != '/') {
            p = "/" + p;
        }
    }
    return p;
}

// Generate a random v4 UUID (8-4-4-4-12 lowercase hex) used in the part-file name
// part-<uuid>-<subtask>-<counter>, matching stock Flink naming.
inline std::string generatePartFileUuid()
{
    std::array<unsigned char, 16> b{};
    std::random_device rd;
    for (auto& byte : b) {
        byte = static_cast<unsigned char>(rd());
    }
    b[6] = static_cast<unsigned char>((b[6] & 0x0F) | 0x40); // version 4
    b[8] = static_cast<unsigned char>((b[8] & 0x3F) | 0x80); // variant 10xx
    static const char* kHex = "0123456789abcdef";
    std::string s;
    s.reserve(36);
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            s += '-';
        }
        s += kHex[(b[i] >> 4) & 0x0F];
        s += kHex[b[i] & 0x0F];
    }
    return s;
}

template <typename IN, typename BucketID>
class BucketsBuilder {
public:
    virtual ~BucketsBuilder() = default;
    virtual Buckets<IN, BucketID>* createBuckets(int subtaskIndex) = 0;
    virtual BucketWriter<IN, BucketID>* createBucketWriter() = 0;
};

template <typename IN, typename BucketID>
class BulkFormatBuilder : public BucketsBuilder<IN, BucketID> {
private:
    Path basePath;
    BucketAssigner<IN, BucketID>* bucketAssigner;
    RollingPolicy<IN, BucketID>* rollingPolicy;
    BucketFactory<IN, BucketID>* bucketFactory;
    OutputFileConfig outputFileConfig;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes;
    std::shared_ptr<omnistream::csv::CsvRowEncoder> csvEncoder_;

    int64_t parseDuration(const std::string& durationStr)
    {
        if (durationStr.empty()) return 0;
        size_t pos = 0;
        int value = std::stoi(durationStr, &pos);
        std::string unit;
        while (pos < durationStr.size() && std::isspace(durationStr[pos])) {
            pos++;
        }
        for (; pos < durationStr.size(); pos++) {
            unit += std::tolower(durationStr[pos]);
        }

        if (unit.empty() || unit == "ms") {
            return value;
        } else if (unit == "s" || unit == "sec" || unit == "secs") {
            return value * 1000;
        } else if (unit == "m" || unit == "min" || unit == "mins") {
            return value * 60 * 1000;
        } else if (unit == "h" || unit == "hr" || unit == "hrs") {
            return value * 60 * 60 * 1000;
        } else {
            return value;
        }
    }

    int64_t parseFileSize(const std::string& fileSizeStr)
    {
        const int64_t DEFAULT_FILE_SIZE = 128LL * 1024LL * 1024LL; // 128MB
        if (fileSizeStr.empty()) return DEFAULT_FILE_SIZE;
        size_t pos = 0;
        int value = std::stoi(fileSizeStr, &pos);
        std::string unit;
        while (pos < fileSizeStr.size() && std::isspace(fileSizeStr[pos])) {
            pos++;
        }
        for (; pos < fileSizeStr.size(); pos++) {
            unit += std::tolower(fileSizeStr[pos]);
        }

        if (unit == "kb" || unit == "k") {
            return value * 1024LL;
        } else if (unit == "mb" || unit == "m") {
            return value * (1024LL * 1024LL);
        } else if (unit == "gb" || unit == "g") {
            return value * (1024LL * 1024LL * 1024LL);
        } else if (unit == "tb" || unit == "t") {
            return value * (1024LL * 1024LL * 1024LL * 1024LL);
        } else {
            return value;
        }
    }

public:
    BulkFormatBuilder(
        const Path& basePath,
        BucketAssigner<IN, BucketID>* assigner,
        RollingPolicy<IN, BucketID>* policy,
        BucketFactory<IN, BucketID>* factory,
        const OutputFileConfig& outputFile,
        std::vector<int> nonPartitionIndexes)
        : basePath(basePath),
          bucketAssigner(assigner),
          rollingPolicy(policy),
          bucketFactory(factory),
          outputFileConfig(outputFile),
          nonPartitionIndexes(nonPartitionIndexes)
    {
    }

    // csv sink 用 vanilla 的 part-<uuid>-<subtask>-<counter> 命名；无 csvSpec 的 filesystem
    // sink 保持历史命名 output-<subtask>-<counter>.txt。
    static OutputFileConfig resolveOutputFileConfig(const nlohmann::json& config)
    {
        if (config.contains("csvSpec")) {
            return OutputFileConfig("part-" + generatePartFileUuid(), "");
        }
        return OutputFileConfig("output", ".txt");
    }

    BulkFormatBuilder(const nlohmann::json& config)
        : basePath(stripFileUriScheme(config["path"].get<std::string>())),
          outputFileConfig(resolveOutputFileConfig(config))
    {
        // Rolling policy mirrors Flink's DefaultRollingPolicy. check-interval only tunes
        // the evaluation timer in stock Flink, so it is unused here; descriptors without
        // inactivity-interval fall back to it.
        const nlohmann::json& rollingPolicyJson = config["rolling-policy"];
        std::string inactivityInterval;
        if (rollingPolicyJson.contains("inactivity-interval")) {
            inactivityInterval = rollingPolicyJson["inactivity-interval"].get<std::string>();
        } else if (rollingPolicyJson.contains("check-interval")) {
            inactivityInterval = rollingPolicyJson["check-interval"].get<std::string>();
        }
        rollingPolicy = new RollingPolicy<IN, BucketID>(
            parseFileSize(rollingPolicyJson["file-size"].get<std::string>()),
            parseDuration(rollingPolicyJson["rollover-interval"].get<std::string>()),
            parseDuration(inactivityInterval));

        auto partitionKeys = config["partitionKeys"].get<std::vector<std::string>>();
        auto partitionIndexes = config["partitionIndexes"].get<std::vector<int>>();

        nonPartitionIndexes = config["nonPartitionIndexes"].get<std::vector<int>>();
        inputTypes = config["inputTypes"].get<std::vector<std::string>>();
        // vanilla 的分区目录默认名可被 partition.default-name 覆盖，描述符按该选项下发。
        std::string defaultPartitionName = config.contains("partitionDefaultName")
                                               ? config["partitionDefaultName"].get<std::string>()
                                               : std::string(BucketAssigner<IN, BucketID>::DEFAULT_PARTITION_NAME);
        bucketAssigner = new BucketAssigner<IN, BucketID>(partitionKeys, partitionIndexes, defaultPartitionName);
        bucketFactory = new BucketFactory<IN, BucketID>();

        // With a csvSpec the FileWriter emits Jackson-aligned CSV instead of naive text.
        if (config.contains("csvSpec")) {
            auto csvSpec = omnistream::csv::CsvSinkSpec::fromJson(config["csvSpec"]);
            csvEncoder_ = std::make_shared<omnistream::csv::CsvRowEncoder>(std::move(csvSpec));
        }
    }

    BucketWriter<IN, BucketID>* createBucketWriter()
    {
        return new BucketWriter<IN, BucketID>(csvEncoder_);
    }

    Buckets<IN, BucketID>* createBuckets(int subtaskIndex) override
    {
        return new Buckets<IN, BucketID>(
            basePath.toString(),
            bucketAssigner,
            bucketFactory,
            createBucketWriter(),
            rollingPolicy,
            subtaskIndex,
            &outputFileConfig,
            nonPartitionIndexes,
            inputTypes);
    }

    ~BulkFormatBuilder()
    {
        delete bucketAssigner;
        delete rollingPolicy;
        delete bucketFactory;
    }
};

#endif // OMNISTREAM_STREAMING_FILE_SINK_H
