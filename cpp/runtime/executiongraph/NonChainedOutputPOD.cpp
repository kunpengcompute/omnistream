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

// NonChainedOutputPOD.cpp
#include "NonChainedOutputPOD.h"
#include <sstream>

namespace omnistream {

NonChainedOutputPOD::NonChainedOutputPOD()
    : supportsUnalignedCheckpoints(false),
      sourceNodeId(0),
      consumerParallelism(0),
      consumerMaxParallelism(0),
      bufferTimeout(0),
      dataSetId(),
      isPersistentDataSet(false),
      partitioner(),
      partitionType(0)
{
}

NonChainedOutputPOD::NonChainedOutputPOD(
    bool supportsUnalignedCheckpoints,
    int sourceNodeId,
    int consumerParallelism,
    int consumerMaxParallelism,
    long bufferTimeout,
    IntermediateDataSetIDPOD dataSetId,
    bool isPersistentDataSet,
    StreamPartitionerPOD partitioner,
    int partitionType)
    : supportsUnalignedCheckpoints(supportsUnalignedCheckpoints),
      sourceNodeId(sourceNodeId),
      consumerParallelism(consumerParallelism),
      consumerMaxParallelism(consumerMaxParallelism),
      bufferTimeout(bufferTimeout),
      dataSetId(dataSetId),
      isPersistentDataSet(isPersistentDataSet),
      partitioner(partitioner),
      partitionType(partitionType)
{
}

NonChainedOutputPOD::NonChainedOutputPOD(const NonChainedOutputPOD& other)
    : supportsUnalignedCheckpoints(other.supportsUnalignedCheckpoints),
      sourceNodeId(other.sourceNodeId),
      consumerParallelism(other.consumerParallelism),
      consumerMaxParallelism(other.consumerMaxParallelism),
      bufferTimeout(other.bufferTimeout),
      dataSetId(other.dataSetId),
      isPersistentDataSet(other.isPersistentDataSet),
      partitioner(other.partitioner),
      partitionType(other.partitionType)
{
}

NonChainedOutputPOD& NonChainedOutputPOD::operator=(const NonChainedOutputPOD& other)
{
    if (this != &other) {
        supportsUnalignedCheckpoints = other.supportsUnalignedCheckpoints;
        sourceNodeId = other.sourceNodeId;
        consumerParallelism = other.consumerParallelism;
        consumerMaxParallelism = other.consumerMaxParallelism;
        bufferTimeout = other.bufferTimeout;
        dataSetId = other.dataSetId;
        isPersistentDataSet = other.isPersistentDataSet;
        partitioner = other.partitioner;
        partitionType = other.partitionType;
    }
    return *this;
}

NonChainedOutputPOD::~NonChainedOutputPOD()
{
}

bool NonChainedOutputPOD::getSupportsUnalignedCheckpoints() const
{
    return supportsUnalignedCheckpoints;
}

int NonChainedOutputPOD::getSourceNodeId() const
{
    return sourceNodeId;
}

void NonChainedOutputPOD::setSourceNodeId(int sourceNodeId_)
{
    this->sourceNodeId = sourceNodeId_;
}

int NonChainedOutputPOD::getConsumerParallelism() const
{
    return consumerParallelism;
}

void NonChainedOutputPOD::setConsumerParallelism(int consumerParallelism_)
{
    this->consumerParallelism = consumerParallelism_;
}

int NonChainedOutputPOD::getConsumerMaxParallelism() const
{
    return consumerMaxParallelism;
}

void NonChainedOutputPOD::setConsumerMaxParallelism(int consumerMaxParallelism_)
{
    this->consumerMaxParallelism = consumerMaxParallelism_;
}

long NonChainedOutputPOD::getBufferTimeout() const
{
    return bufferTimeout;
}

void NonChainedOutputPOD::setBufferTimeout(long bufferTimeout_)
{
    this->bufferTimeout = bufferTimeout_;
}

IntermediateDataSetIDPOD NonChainedOutputPOD::getDataSetId() const
{
    return dataSetId;
}

void NonChainedOutputPOD::setDataSetId(const IntermediateDataSetIDPOD& dataSetId_)
{
    this->dataSetId = dataSetId_;
}

bool NonChainedOutputPOD::getIsPersistentDataSet() const
{
    return isPersistentDataSet;
}

void NonChainedOutputPOD::setIsPersistentDataSet(bool isPersistentDataSet_)
{
    this->isPersistentDataSet = isPersistentDataSet_;
}

StreamPartitionerPOD NonChainedOutputPOD::getPartitioner() const
{
    return partitioner;
}

void NonChainedOutputPOD::setPartitioner(const StreamPartitionerPOD& partitioner_)
{
    this->partitioner = partitioner_;
}

int NonChainedOutputPOD::getPartitionType() const
{
    return partitionType;
}

void NonChainedOutputPOD::setPartitionType(int partitionType_)
{
    this->partitionType = partitionType_;
}

std::string NonChainedOutputPOD::toString() const
{
    std::stringstream ss;
    ss << "NonChainedOutputPOD{"
       << "sourceNodeId=" << sourceNodeId << ", consumerParallelism=" << consumerParallelism
       << ", consumerMaxParallelism=" << consumerMaxParallelism << ", bufferTimeout=" << bufferTimeout
       << ", dataSetId=" << dataSetId.toString() << ", isPersistentDataSet=" << isPersistentDataSet
       << ", partitioner=" << partitioner.toString() << ", partitionType=" << partitionType << "}";
    return ss.str();
}

} // namespace omnistream
