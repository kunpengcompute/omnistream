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

#include <nlohmann/json.hpp>

class RocksDBMemoryConfiguration {
public:
    RocksDBMemoryConfiguration()
        : usingManagedMemory(true),
          writeBufferRatio(0.5),
          highPriorityPoolRatio(0.1),
          usingPartitionedIndexFilters(false),
          usingFixedMemoryPerSlot(false)
    {
    }

    bool isUsingManagedMemory() const
    {
        return usingManagedMemory;
    }

    double getWriteBufferRatio() const
    {
        return writeBufferRatio;
    }

    double getHighPriorityPoolRatio() const
    {
        return highPriorityPoolRatio;
    }

    bool isUsingPartitionedIndexFilters() const
    {
        return usingPartitionedIndexFilters;
    }

    bool isUsingFixedMemoryPerSlot() const
    {
        return usingFixedMemoryPerSlot;
    }

    friend void to_json(nlohmann::json& json, const RocksDBMemoryConfiguration& config)
    {
        json = nlohmann::json{
            {"usingManagedMemory", config.usingManagedMemory},
            {"writeBufferRatio", config.writeBufferRatio},
            {"highPriorityPoolRatio", config.highPriorityPoolRatio},
            {"usingPartitionedIndexFilters", config.usingPartitionedIndexFilters},
            {"usingFixedMemoryPerSlot", config.usingFixedMemoryPerSlot}};
    }

    friend void from_json(const nlohmann::json& json, RocksDBMemoryConfiguration& config)
    {
        config.usingManagedMemory = json.value("usingManagedMemory", true);
        config.writeBufferRatio = json.value("writeBufferRatio", 0.5);
        config.highPriorityPoolRatio = json.value("highPriorityPoolRatio", 0.1);
        config.usingPartitionedIndexFilters = json.value("usingPartitionedIndexFilters", false);
        config.usingFixedMemoryPerSlot = json.value("usingFixedMemoryPerSlot", false);
    }

private:
    bool usingManagedMemory;
    // todo: MemorySize fixedMemoryPerSlot
    double writeBufferRatio;
    double highPriorityPoolRatio;
    bool usingPartitionedIndexFilters;
    bool usingFixedMemoryPerSlot;
};
