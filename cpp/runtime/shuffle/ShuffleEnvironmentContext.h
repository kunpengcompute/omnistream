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

#ifndef SHUFFLEENVIRONMENTCONTEXT_H
#define SHUFFLEENVIRONMENTCONTEXT_H
#include <executiongraph/descriptor/ResourceIDPOD.h>
#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>

namespace omnistream {
class ShuffleEnvironmentContext {
public:
    // Default constructor
    ShuffleEnvironmentContext() : memorySize(0), pageSize(0), requestSegmentsTimeoutMillis(0)
    {
    }

    // Full argument constructor
    ShuffleEnvironmentContext(
        const ResourceIDPOD& resourceID,
        long memorySize,
        int pageSize,
        long requestSegmentsTimeoutMillis,
        int networkbuffersPerChannel,
        int partitionRequestInitialBackoff,
        int partitionRequestMaxBackoff,
        int floatingNetworkbuffersPerGate,
        int segmentSize,
        int numberofSegmentsGlobal,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism,
        int maxBuffersPerChannel)
        : resourceID(resourceID),
          memorySize(memorySize),
          pageSize(pageSize),
          requestSegmentsTimeoutMillis(requestSegmentsTimeoutMillis),
          networkbuffersPerChannel(networkbuffersPerChannel),
          partitionRequestInitialBackoff(partitionRequestInitialBackoff),
          partitionRequestMaxBackoff(partitionRequestMaxBackoff),
          floatingNetworkbuffersPerGate(floatingNetworkbuffersPerGate),
          segmentSize(segmentSize),
          numberofSegmentsGlobal(numberofSegmentsGlobal),
          sortShuffleMinBuffers(sortShuffleMinBuffers),
          sortShuffleMinParallelism(sortShuffleMinParallelism),
          maxBuffersPerChannel(maxBuffersPerChannel)
    {
    }

    // Copy constructor
    ShuffleEnvironmentContext(const ShuffleEnvironmentContext& other)
        : resourceID(other.resourceID),
          memorySize(other.memorySize),
          pageSize(other.pageSize),
          requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
          sortShuffleMinBuffers(other.sortShuffleMinBuffers),
          sortShuffleMinParallelism(other.sortShuffleMinParallelism),
          maxBuffersPerChannel(other.maxBuffersPerChannel)
    {
    }

    ShuffleEnvironmentContext& operator=(const ShuffleEnvironmentContext& other)
    {
        resourceID = other.resourceID;
        memorySize = other.memorySize;
        pageSize = other.pageSize;
        requestSegmentsTimeoutMillis = other.partitionRequestMaxBackoff;
        sortShuffleMinBuffers = other.sortShuffleMinBuffers;
        sortShuffleMinParallelism = other.sortShuffleMinParallelism;
        maxBuffersPerChannel = other.maxBuffersPerChannel;
        return *this;
    }

    // Getters
    const ResourceIDPOD& getResourceID() const
    {
        return resourceID;
    }
    long getMemorySize() const
    {
        return memorySize;
    }
    int getPageSize() const
    {
        return pageSize;
    }
    long getRequestSegmentsTimeoutMillis() const
    {
        return requestSegmentsTimeoutMillis;
    }

    int getNetworkBuffersPerChannel() const
    {
        return networkbuffersPerChannel;
    }
    int getPartitionRequestInitialBackoff() const
    {
        return partitionRequestInitialBackoff;
    }
    int getPartitionRequestMaxBackoff() const
    {
        return partitionRequestMaxBackoff;
    }
    int getFloatingNetworkBuffersPerGate() const
    {
        return floatingNetworkbuffersPerGate;
    }
    int getSegmentSize() const
    {
        return segmentSize;
    }
    int getNumberofSegmentsGlobal() const
    {
        return numberofSegmentsGlobal;
    }
    int getsortShuffleMinBuffers() const
    {
        return sortShuffleMinBuffers;
    }
    int getsortShuffleMinParallelism() const
    {
        return sortShuffleMinParallelism;
    }
    int getmaxBuffersPerChannel() const
    {
        return maxBuffersPerChannel;
    }

    // Setters

    void setResourceID(const ResourceIDPOD& resourceID_)
    {
        this->resourceID = resourceID_;
    }
    void setMemorySize(long memorySize_)
    {
        this->memorySize = memorySize_;
    }
    void setPageSize(int pageSize_)
    {
        this->pageSize = pageSize_;
    }
    void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis_)
    {
        this->requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis_;
    }
    void setsortShuffleMinParallelism(int sortShuffleMinParallelism_)
    {
        this->sortShuffleMinParallelism = sortShuffleMinParallelism_;
    }
    void setsortShuffleMinBuffers(int sortShuffleMinBuffers_)
    {
        this->sortShuffleMinBuffers = sortShuffleMinBuffers_;
    }

    void setmaxBuffersPerChannel(int maxBuffersPerChannel)
    {
        this->maxBuffersPerChannel = maxBuffersPerChannel;
    }

    // toString method
    std::string toString() const
    {
        return "Resource ID: " + resourceID.toString() + // Assuming ResourceIDPOD has a toString()
               ", Memory Size: " + std::to_string(memorySize) + ", Page Size: " + std::to_string(pageSize) +
               ", Request Timeout: " + std::to_string(requestSegmentsTimeoutMillis);
    }

    // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        ShuffleEnvironmentContext,
        resourceID,
        memorySize,
        pageSize,
        requestSegmentsTimeoutMillis,
        networkbuffersPerChannel,
        partitionRequestInitialBackoff,
        partitionRequestMaxBackoff,
        floatingNetworkbuffersPerGate,
        segmentSize,
        numberofSegmentsGlobal,
        sortShuffleMinBuffers,
        sortShuffleMinParallelism,
        maxBuffersPerChannel)
private:
    ResourceIDPOD resourceID;
    long memorySize;
    int pageSize;
    long requestSegmentsTimeoutMillis;

    int networkbuffersPerChannel;
    int partitionRequestInitialBackoff;
    int partitionRequestMaxBackoff;
    int floatingNetworkbuffersPerGate;
    int segmentSize;
    int numberofSegmentsGlobal;

    int sortShuffleMinBuffers;
    int sortShuffleMinParallelism;

    int maxBuffersPerChannel;
};

} // namespace omnistream

#endif
