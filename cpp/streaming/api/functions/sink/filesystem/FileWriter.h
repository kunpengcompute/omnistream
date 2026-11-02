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

#ifndef OMNISTREAM_IN_PROGRESS_FILE_WRITER_H
#define OMNISTREAM_IN_PROGRESS_FILE_WRITER_H

#include <iostream>
#include <fstream>
#include <memory>
#include "table/data/util/VectorBatchUtil.h"
#include "formats/csv/CsvRowEncoder.h"

// This class combines InProgressFileWriter and PartFileInfo.
template <typename IN, typename BucketID>
class FileWriter {
public:
    FileWriter(
        const std::string& path,
        long createdTime,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes,
        std::shared_ptr<omnistream::csv::CsvRowEncoder> encoder = nullptr)
        : filePath(path),
          createdTime(createdTime),
          nonPartitionIndexes(nonPartitionIndexes),
          inputTypes(inputTypes),
          encoder_(std::move(encoder))
    {
        outFile.open(filePath, std::ios::out | std::ios::app);
        if (!outFile.is_open()) {
            THROW_RUNTIME_ERROR("FileWriter: failed to open output file: " << filePath);
        }
        // Surface write failures (disk full, broken pipe, ...) as exceptions so
        // the task is notified instead of silently dropping records.
        outFile.exceptions(std::ofstream::badbit);
    }

    void write(IN batch, int rowId, long currentTime)
    {
        if (!outFile.is_open()) {
            THROW_RUNTIME_ERROR("FileWriter: output file is not open: " << filePath);
        }
        auto vb = reinterpret_cast<omnistream::VectorBatch*>(batch);
        if (encoder_) {
            // Native CSV path: encoder produces the field content; write by explicit
            // length so embedded NUL bytes are preserved.
            std::string row = encoder_->encode(vb, rowId);
            outFile.write(row.data(), static_cast<std::streamsize>(row.size()));
        } else {
            // Legacy naive-text path for descriptors without a csvSpec.
            for (size_t i = 0; i < nonPartitionIndexes.size(); ++i) {
                size_t colId = nonPartitionIndexes[i];
                auto val = VectorBatchUtil::getValueAtAsStr(vb, colId, rowId, inputTypes);
                outFile << val;
                if (colId != nonPartitionIndexes.size() - 1) {
                    outFile << ",";
                }
            }
        }
        outFile << "\n";
        lastUpdatedTime = currentTime;
    };

    void flush()
    {
        outFile.flush();
    }

    void close()
    {
        if (outFile.is_open()) {
            outFile.close();
        }
    }

    int64_t getSize()
    {
        if (!outFile.is_open()) {
            return 0;
        }
        auto pos = outFile.tellp();
        return pos == std::streampos(-1) ? 0 : static_cast<int64_t>(pos);
    }

    long getCreationTime()
    {
        return createdTime;
    }

    long getLastUpdateTime()
    {
        return lastUpdatedTime;
    }

    ~FileWriter()
    {
        LOG("FileWriter => Destructor");
        close();
    }

private:
    std::string filePath;
    std::ofstream outFile;
    long createdTime;
    long lastUpdatedTime;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes = {};
    std::shared_ptr<omnistream::csv::CsvRowEncoder> encoder_;
};

#endif // OMNISTREAM_IN_PROGRESS_FILE_WRITER_H
