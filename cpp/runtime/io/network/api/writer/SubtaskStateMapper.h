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

#ifndef SUBTASKSTATEMAPPER_H
#define SUBTASKSTATEMAPPER_H
#include "../../../../checkpoint/RescaleMappings.h"

namespace omnistream {
class SubtaskStateMapper {
public:
    enum Type {
        ARBITRARY,
        FIRST,
        FULL,
        RANGE,
        ROUND_ROBIN,
        UNSUPPORTED
    };

    SubtaskStateMapper() = default;
    virtual ~SubtaskStateMapper() = default;

    virtual std::vector<int> getOldSubtasks(int newSubtaskIndex, int oldNumberOfSubtasks, int newNumberOfSubtasks)
    {
        return {};
    };

    RescaleMappings getNewToOldSubtasksMapping(int oldParallelism, int newParallelism)
    {
        std::vector<std::vector<int>> mappings;
        for (int i = 0; i < newParallelism; ++i) {
            mappings.push_back(getOldSubtasks(i, oldParallelism, newParallelism));
        }
        return RescaleMappings::of(mappings, oldParallelism);
    }

    virtual bool isAmbiguous() const
    {
        return false;
    }

    static SubtaskStateMapper* create(Type type);
};

class RoundRobinMapper : public SubtaskStateMapper {
public:
    std::vector<int> getOldSubtasks(int newSubtaskIndex, int oldNumberOfSubtasks, int newNumberOfSubtasks) override
    {
        std::vector<int> subtasks;
        for (int subtask = newSubtaskIndex; subtask < oldNumberOfSubtasks; subtask += newNumberOfSubtasks) {
            subtasks.push_back(subtask);
        }
        return subtasks;
    }
};

class ArbitraryMapper : public SubtaskStateMapper {
public:
    std::vector<int> getOldSubtasks(int newSubtaskIndex, int oldNumberOfSubtasks, int newNumberOfSubtasks) override
    {
        RoundRobinMapper rrMapper;
        return rrMapper.getOldSubtasks(newSubtaskIndex, oldNumberOfSubtasks, newNumberOfSubtasks);
    }
};

class UnsupportedMapper : public SubtaskStateMapper {
    std::vector<int> getOldSubtasks(int newSubtaskIndex, int oldNumberOfSubtasks, int newNumberOfSubtasks) override
    {
        throw std::runtime_error(
            "Cannot rescale the given pointwise partitioner.\n"
            "Did you change the partitioner to forward or rescale?\n"
            "It may also help to add an explicit shuffle().");
    }
};

} // namespace omnistream

#endif
