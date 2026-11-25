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

#ifndef OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H
#define OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H

#include <memory>
#include <string>
#include <sstream>
#include <stdexcept>

#include <execution/ExecutionState.h>
#include <executiongraph/common/IntermediateDataSetIDPOD.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <utils/exception/Throwable.h>

namespace omnistream {

class PartitionProducerStateProvider {
public:
    class ResponseHandle {
    public:
        virtual ExecutionState getConsumerExecutionState() = 0;

        virtual void cancelConsumption() = 0;
        virtual void failConsumption(std::shared_ptr<Throwable> cause) = 0;
        virtual ~ResponseHandle() = default;
        virtual std::string toString() const = 0;
    };

    class Consumer {
    public:
        virtual void accept(std::shared_ptr<ResponseHandle> responseHandle) = 0;
        virtual ~Consumer() = default;
        virtual std::string toString() const = 0;
    };

    virtual void requestPartitionProducerState(
        std::shared_ptr<IntermediateDataSetIDPOD> intermediateDataSetId,
        std::shared_ptr<ResultPartitionIDPOD> resultPartitionId,
        std::shared_ptr<Consumer> responseConsumer) = 0;

    virtual ~PartitionProducerStateProvider() = default;
    virtual std::string toString() const = 0;
};

} // namespace omnistream

#endif // OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H
