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

#ifndef FLINK_TNEL_CHECKPOINTSTREAMWITHRESULTPROVIDER_H
#define FLINK_TNEL_CHECKPOINTSTREAMWITHRESULTPROVIDER_H

#include "CheckpointStreamFactory.h"

class CheckpointStreamWithResultProvider {
public:
    class PrimaryStreamOnly;

    virtual ~CheckpointStreamWithResultProvider() = default;

    virtual CheckpointStateOutputStream& getCheckpointOutputStream() = 0;

    virtual void close()
    {
        getCheckpointOutputStream().Close();
    }

    static std::shared_ptr<CheckpointStreamWithResultProvider> createSimpleStream(
        CheckpointedStateScope checkpointedStateScope, std::shared_ptr<CheckpointStreamFactory> primaryStreamFactory);
};

class CheckpointStreamWithResultProvider::PrimaryStreamOnly : public CheckpointStreamWithResultProvider {
public:
    explicit PrimaryStreamOnly(std::shared_ptr<CheckpointStateOutputStream> outputStream)
        : outputStream_(std::move(outputStream))
    {
    }

    CheckpointStateOutputStream& getCheckpointOutputStream() override
    {
        return *outputStream_;
    }

private:
    std::shared_ptr<CheckpointStateOutputStream> outputStream_;
};

inline std::shared_ptr<CheckpointStreamWithResultProvider> CheckpointStreamWithResultProvider::createSimpleStream(
    CheckpointedStateScope checkpointedStateScope, std::shared_ptr<CheckpointStreamFactory> primaryStreamFactory)
{
    auto* rawOut = primaryStreamFactory->createCheckpointStateOutputStream(checkpointedStateScope);
    auto outPtr = std::shared_ptr<CheckpointStateOutputStream>(rawOut);
    return std::make_shared<PrimaryStreamOnly>(std::move(outPtr));
}

#endif // FLINK_TNEL_CHECKPOINTSTREAMWITHRESULTPROVIDER_H
