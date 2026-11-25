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

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

class RocksDbStringAppendOperator : public rocksdb::MergeOperator {
public:
    explicit RocksDbStringAppendOperator(char delimiter) : delimiter(delimiter)
    {
    }

    bool FullMergeV2(const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override
    {
        merge_out->new_value.clear();
        size_t total_size = 0;
        if (merge_in.existing_value) {
            total_size += merge_in.existing_value->size();
        }
        for (const auto& op : merge_in.operand_list) {
            if (total_size > 0) {
                total_size += 1; // delimiter
            }
            total_size += op.size();
        }

        merge_out->new_value.reserve(total_size);
        if (merge_in.existing_value) {
            merge_out->new_value.append(merge_in.existing_value->data(), merge_in.existing_value->size());
        }
        for (const auto& op : merge_in.operand_list) {
            if (!merge_out->new_value.empty()) {
                merge_out->new_value.append(1, delimiter);
            }
            merge_out->new_value.append(op.data(), op.size());
        }
        return true;
    }

    bool PartialMergeMulti(
        const rocksdb::Slice& key,
        const std::deque<rocksdb::Slice>& operand_list,
        std::string* new_value,
        rocksdb::Logger* logger) const override
    {
        return false;
    }

    [[nodiscard]] const char* Name() const override
    {
        return "RocksDbStringAppendOperator";
    }

private:
    char delimiter; // The delimiter is inserted between elements
};
