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

#ifndef FLINK_TNEL_TASKINFOIMPL_H
#define FLINK_TNEL_TASKINFOIMPL_H

#include <string>
#include <cstring>
#include <vector>
#include "TaskInfo.h"

class TaskInfoImpl : public TaskInfo {
public:
    TaskInfoImpl(
        std::string taskName, int maxNumberOfParallelSubtasks, int numberOfParallelSubtasks, int indexOfSubtask)
        : taskName(taskName),
          maxNumberOfParallelSubtasks(maxNumberOfParallelSubtasks),
          indexOfSubtask(indexOfSubtask),
          numberOfParallelSubtasks(numberOfParallelSubtasks)
    {
        const char* envVar = std::getenv("RocksDBBackend");
        if (envVar != nullptr) {
            if (strlen(envVar) != 0 && strcmp(envVar, "true") == 0) {
                stateBackend = "EmbeddedRocksDBStateBackend";
            }
        }
    };

    TaskInfoImpl(
        std::string taskName,
        int maxNumberOfParallelSubtasks,
        int numberOfParallelSubtasks,
        int indexOfSubtask,
        std::string stateBackend)
        : taskName(taskName),
          maxNumberOfParallelSubtasks(maxNumberOfParallelSubtasks),
          indexOfSubtask(indexOfSubtask),
          numberOfParallelSubtasks(numberOfParallelSubtasks),
          stateBackend(stateBackend) {};

    TaskInfoImpl(
        std::string taskName,
        int maxNumberOfParallelSubtasks,
        int numberOfParallelSubtasks,
        int indexOfSubtask,
        std::string stateBackend,
        std::vector<std::string> backendHomes)
        : taskName(taskName),
          maxNumberOfParallelSubtasks(maxNumberOfParallelSubtasks),
          indexOfSubtask(indexOfSubtask),
          numberOfParallelSubtasks(numberOfParallelSubtasks),
          stateBackend(stateBackend)
    {
        if (backendHomes.size() == 0) {
            backendHome = "/tmp/rocksdb";
        } else {
            backendHome = backendHomes[0];
        }
    };

    std::string getTaskName() override
    {
        return taskName;
    };
    int getMaxNumberOfParallelSubtasks() override
    {
        return maxNumberOfParallelSubtasks;
    };
    int getIndexOfThisSubtask() override
    {
        return indexOfSubtask;
    };
    int getNumberOfParallelSubtasks() override
    {
        return numberOfParallelSubtasks;
    };
    std::string getStateBackend() override
    {
        return stateBackend;
    }
    std::string getBackendHome() override
    {
        return backendHome;
    }

private:
    std::string taskName;
    int maxNumberOfParallelSubtasks;
    int indexOfSubtask;
    int numberOfParallelSubtasks;
    std::string stateBackend = "HashMapStateBackend"; // HashMapStateBackend
    std::string backendHome = "/tmp/rocksdb";
};

#endif // FLINK_TNEL_TASKINFOIMPL_H
