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

#ifndef OMNISTREAM_BUCKET_ASSIGNER_H
#define OMNISTREAM_BUCKET_ASSIGNER_H

#include <nlohmann/json.hpp>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/PartitionPathUtils.h"

class BucketAssignerContext {
public:
    virtual ~BucketAssignerContext() = default;
    virtual long getCurrentProcessingTime() const = 0;
    virtual long getCurrentWatermark() const = 0;
    virtual long getTimestamp() const = 0;
};

template <typename IN, typename BucketID>
class BucketAssigner {
public:
    // vanilla 的默认分区名。
    static constexpr const char* DEFAULT_PARTITION_NAME = "__DEFAULT_PARTITION__";

    BucketAssigner(std::vector<std::string> keys, std::vector<int> indexes)
        : BucketAssigner(std::move(keys), std::move(indexes), DEFAULT_PARTITION_NAME)
    {
    }

    BucketAssigner(std::vector<std::string> keys, std::vector<int> indexes, std::string defaultPartitionName)
        : partitionKeys(std::move(keys)),
          partitionIndexes(std::move(indexes)),
          defaultPartitionName(std::move(defaultPartitionName))
    {
        if (partitionKeys.size() != partitionIndexes.size()) {
            throw std::runtime_error("mismatched keys/indexes size");
        }
    }

    BucketID getBucketId(IN batch, int rowId, BucketAssignerContext* context)
    {
        std::vector<std::pair<std::string, std::string>> partitionSpec;
        auto vb = reinterpret_cast<omnistream::VectorBatch*>(batch);

        for (size_t i = 0; i < partitionKeys.size(); ++i) {
            const int columnIndex = partitionIndexes[i];
            // vanilla 把 NULL 与空串都替换成 partition.default-name 再拼目录；native 原先把
            // NULL 输出成字面量 "NULL"、空串直接抛异常，这里与 vanilla 对齐。
            std::string val = vb->Get(columnIndex)->IsNull(rowId)
                                  ? defaultPartitionName
                                  : VectorBatchUtil::getValueAtAsStr(vb, columnIndex, rowId);
            if (val.empty()) {
                val = defaultPartitionName;
            }
            partitionSpec.emplace_back(partitionKeys[i], std::move(val));
        }

        auto partitionPath = PartitionPathUtils::generatePartitionPath(partitionSpec);
        return partitionPath;
    };

private:
    std::vector<std::string> partitionKeys;
    std::vector<int> partitionIndexes;
    std::string defaultPartitionName;
};

#endif // OMNISTREAM_BUCKET_ASSIGNER_H
