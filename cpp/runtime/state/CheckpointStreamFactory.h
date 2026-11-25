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

#ifndef FLINK_TNEL_CHECKPOINTSTREAMFACTORY_H
#define FLINK_TNEL_CHECKPOINTSTREAMFACTORY_H
#include <memory>
#include <vector>
#include <stdexcept>
#include "StreamStateHandle.h"

enum class CheckpointedStateScope {
    EXCLUSIVE,
    SHARED
};

class FSDataOutputStream {
public:
    virtual long GetPos() = 0;
    virtual void Flush() = 0;
    virtual void Sync() = 0;
    virtual void Close() = 0;
    virtual void Write(const void* data, size_t length) = 0;
    virtual ~FSDataOutputStream() = default;
};

class CheckpointStateOutputStream : public FSDataOutputStream {
public:
    virtual std::shared_ptr<StreamStateHandle> CloseAndGetHandle(char* dataStream) = 0;
    void Close() override = 0;
};

class CheckpointStreamFactory {
public:
    virtual ~CheckpointStreamFactory() = default;

    virtual CheckpointStateOutputStream* createCheckpointStateOutputStream(CheckpointedStateScope scope) = 0;

    virtual bool canFastDuplicate(StreamStateHandle* stateHandle, CheckpointedStateScope scope) = 0;

    virtual std::vector<std::shared_ptr<StreamStateHandle>> duplicate(
        std::vector<StreamStateHandle*>& stateHandles, CheckpointedStateScope scope) = 0;
};

#endif // FLINK_TNEL_CHECKPOINTSTREAMFACTORY_H
