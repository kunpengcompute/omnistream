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

#ifndef TASKMANAGERSERVICECONFIGURATIONPOD_H
#define TASKMANAGERSERVICECONFIGURATIONPOD_H

#include <string>
#include <iostream>
#include <nlohmann/json.hpp>

#include "ResourceIDPOD.h"
namespace omnistream {
class TaskManagerServiceConfigurationPOD {
public:
    // Default constructor
    TaskManagerServiceConfigurationPOD()
        : memorySize(0),
          pageSize(0),
          requestSegmentsTimeoutMillis(0),
          numIoThreads(0),
          externalAddress(""),
          localCommunicationOnly(false),
          networkbuffersPerChannel(0),
          partitionRequestMaxBackoff(0),
          partitionRequestInitialBackoff(0),
          floatingNetworkbuffersPerGate(0),
          segmentSize(0),
          numberofSegmentsGlobal(0),
          sortShuffleMinBuffers(0),
          sortShuffleMinParallelism(0),
          maxBuffersPerChannel(0)
    {
    }

    // Copy constructor
    TaskManagerServiceConfigurationPOD& operator=(const TaskManagerServiceConfigurationPOD&) = delete;
    TaskManagerServiceConfigurationPOD(const TaskManagerServiceConfigurationPOD& other)
        : resourceID(other.resourceID),
          memorySize(other.memorySize),
          pageSize(other.pageSize),
          requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
          numIoThreads(other.numIoThreads),
          externalAddress(other.externalAddress),
          localCommunicationOnly(other.localCommunicationOnly),
          networkbuffersPerChannel(other.networkbuffersPerChannel),
          partitionRequestMaxBackoff(other.partitionRequestMaxBackoff),
          partitionRequestInitialBackoff(other.partitionRequestInitialBackoff),
          floatingNetworkbuffersPerGate(other.floatingNetworkbuffersPerGate),
          segmentSize(other.segmentSize),
          numberofSegmentsGlobal(other.numberofSegmentsGlobal),
          sortShuffleMinBuffers(other.sortShuffleMinBuffers),
          sortShuffleMinParallelism(other.sortShuffleMinParallelism),
          maxBuffersPerChannel(other.maxBuffersPerChannel)
    {
    }
    TaskManagerServiceConfigurationPOD(
        const ResourceIDPOD& resource_id,
        long memory_size,
        int page_size,
        long request_segments_timeout_millis,
        int num_io_threads,
        const std::string& external_address,
        bool local_communication_only,
        int networkbuffers_per_channel,
        int partition_request_initial_backoff,
        int partition_request_max_backoff,
        int floating_networkbuffers_per_gate,
        int segment_size,
        int numberof_segments_global,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism,
        int maxBuffersPerChannel)
        : resourceID(resource_id),
          memorySize(memory_size),
          pageSize(page_size),
          requestSegmentsTimeoutMillis(request_segments_timeout_millis),
          numIoThreads(num_io_threads),
          externalAddress(external_address),
          localCommunicationOnly(local_communication_only),
          networkbuffersPerChannel(networkbuffers_per_channel),
          partitionRequestMaxBackoff(partition_request_max_backoff),
          partitionRequestInitialBackoff(partition_request_initial_backoff),
          floatingNetworkbuffersPerGate(floating_networkbuffers_per_gate),
          segmentSize(segment_size),
          numberofSegmentsGlobal(numberof_segments_global),
          sortShuffleMinBuffers(sortShuffleMinBuffers),
          sortShuffleMinParallelism(sortShuffleMinParallelism),
          maxBuffersPerChannel(maxBuffersPerChannel)
    {
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
    int getNumIoThreads() const
    {
        return numIoThreads;
    }
    const std::string& getExternalAddress() const
    {
        return externalAddress;
    }
    bool isLocalCommunicationOnly() const
    {
        return localCommunicationOnly;
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

    int getsortShuffleMinParallelism() const
    {
        return sortShuffleMinParallelism;
    }

    int getsortShuffleMinBuffers() const
    {
        return sortShuffleMinBuffers;
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
    void setNumIoThreads(int numIoThreads_)
    {
        this->numIoThreads = numIoThreads_;
    }
    void setExternalAddress(const std::string& externalAddress_)
    {
        this->externalAddress = externalAddress_;
    }
    void setLocalCommunicationOnly(bool localCommunicationOnly_)
    {
        this->localCommunicationOnly = localCommunicationOnly_;
    }

    void setnetworkbuffersPerChannel(int networkbuffersPerChannel_)
    {
        this->networkbuffersPerChannel = networkbuffersPerChannel_;
    }
    void setpartitionRequestInitialBackoff(int setpartitionRequestInitialBackoff_)
    {
        this->partitionRequestInitialBackoff = setpartitionRequestInitialBackoff_;
    }
    void setpartitionRequestMaxBackoff(int partitionRequestMaxBackoff_)
    {
        this->partitionRequestMaxBackoff = partitionRequestMaxBackoff_;
    }
    void setfloatingNetworkbuffersPerGate(int floatingNetworkbuffersPerGate_)
    {
        this->floatingNetworkbuffersPerGate = floatingNetworkbuffersPerGate_;
    }
    void setsegmentSize(int segmentSize_)
    {
        this->segmentSize = segmentSize_;
    }
    void setnumberofSegmentsGlobal(int numberofSegmentsGlobal_)
    {
        this->numberofSegmentsGlobal = numberofSegmentsGlobal_;
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
        return "Resource ID: " + resourceID.toString() + // Assuming ResourceIDPOJO has a toString()
               ", Memory Size: " + std::to_string(memorySize) + ", Page Size: " + std::to_string(pageSize) +
               ", Request Timeout: " + std::to_string(requestSegmentsTimeoutMillis) +
               ", IO Threads: " + std::to_string(numIoThreads) + ", External Address: " + externalAddress +
               ", Local Communication Only: " + (localCommunicationOnly ? "true" : "false") +
               ", networkbuffersPerChannel: " + std::to_string(networkbuffersPerChannel) +
               ", partitionRequestMaxBackoff: " + std::to_string(partitionRequestMaxBackoff) +
               ", floatingNetworkbuffersPerGate: " + std::to_string(floatingNetworkbuffersPerGate) +
               ", numberofSegmentsGlobal: " + std::to_string(numberofSegmentsGlobal) +
               ", segmentSize: " + std::to_string(segmentSize) +
               ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel);
    }

    // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        TaskManagerServiceConfigurationPOD,
        resourceID,
        memorySize,
        pageSize,
        requestSegmentsTimeoutMillis,
        numIoThreads,
        externalAddress,
        localCommunicationOnly,
        networkbuffersPerChannel,
        partitionRequestInitialBackoff,
        partitionRequestMaxBackoff,
        floatingNetworkbuffersPerGate,
        numberofSegmentsGlobal,
        segmentSize,
        sortShuffleMinParallelism,
        sortShuffleMinBuffers,
        maxBuffersPerChannel)
private:
    ResourceIDPOD resourceID;
    long memorySize;
    int pageSize;
    long requestSegmentsTimeoutMillis;
    int numIoThreads;
    std::string externalAddress;
    bool localCommunicationOnly;

    int networkbuffersPerChannel;
    int partitionRequestMaxBackoff;
    int partitionRequestInitialBackoff;
    int floatingNetworkbuffersPerGate;
    int segmentSize;
    int numberofSegmentsGlobal;

    int sortShuffleMinBuffers;
    int sortShuffleMinParallelism;
    int maxBuffersPerChannel;
};

} // namespace omnistream

#endif // TASKMANAGERSERVICECONFIGURATIONPOD_H
