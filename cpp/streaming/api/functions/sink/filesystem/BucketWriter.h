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

#ifndef OMNISTREAM_BUCKET_WRITER_H
#define OMNISTREAM_BUCKET_WRITER_H

#include <string>
#include <filesystem>
#include <memory>
#include "FileWriter.h"

namespace fs = std::filesystem;

template <typename IN, typename BucketID>
class BucketWriter {
public:
    BucketWriter() = default;
    explicit BucketWriter(std::shared_ptr<omnistream::csv::CsvRowEncoder> encoder) : encoder_(std::move(encoder))
    {
    }

    FileWriter<IN, BucketID>* openNewInProgressFile(
        BucketID bucketId,
        const std::string& path,
        long currentTime,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
    {
        fs::path targetPath(path);
        fs::path parentDir = targetPath.parent_path();

        if (!fs::exists(parentDir)) {
            std::error_code ec;
            if (!fs::create_directories(parentDir, ec)) {
                throw std::ios_base::failure(
                    "Failed to create directory: " + parentDir.string() + ", error: " + ec.message());
            }
        }

        return new FileWriter<IN, BucketID>(path, currentTime, nonPartitionIndexes, inputTypes, encoder_);
    }

private:
    std::shared_ptr<omnistream::csv::CsvRowEncoder> encoder_;
};

#endif // OMNISTREAM_BUCKET_WRITER_H
