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

#ifndef OMNISHUFFLEENVIRONMENTCONFIGURATION_H
#define OMNISHUFFLEENVIRONMENTCONFIGURATION_H
#include <executiongraph/descriptor/ResourceIDPOD.h>

namespace omnistream {
class OmniShuffleEnvironmentConfiguration {
public:
    // Default constructor
    OmniShuffleEnvironmentConfiguration()
        : numNetworkBuffers(0),
          networkBufferSize(0),
          requestSegmentsTimeoutMillis(0),
          networkBuffersPerChannel(0),
          partitionRequestMaxBackoff(0),
          sortShuffleMinBuffers(0),
          sortShuffleMinParallelism(0),
          maxBuffersPerChannel(0)
    {
    }

    // Full argument constructor
    OmniShuffleEnvironmentConfiguration(
        int numNetworkBuffers,
        int networkBufferSize,
        long requestSegmentsTimeoutMillis,
        int networkBuffersPerChannel,
        int partitionRequestInitialBackoff,
        int partitionRequestMaxBackoff,
        int floatingNetworkBuffersPerGate,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism,
        int maxBuffersPerChannel)
        : numNetworkBuffers(numNetworkBuffers),
          networkBufferSize(networkBufferSize),
          requestSegmentsTimeoutMillis(requestSegmentsTimeoutMillis),
          networkBuffersPerChannel(networkBuffersPerChannel),
          partitionRequestMaxBackoff(partitionRequestMaxBackoff),
          partitionRequestInitialBackoff(partitionRequestInitialBackoff),
          floatingNetworkBuffersPerGate(floatingNetworkBuffersPerGate),
          sortShuffleMinBuffers(sortShuffleMinBuffers),
          sortShuffleMinParallelism(sortShuffleMinParallelism),
          maxBuffersPerChannel(maxBuffersPerChannel)
    {
    }

    // Copy constructor
    OmniShuffleEnvironmentConfiguration(const OmniShuffleEnvironmentConfiguration& other)
        : numNetworkBuffers(other.numNetworkBuffers),
          networkBufferSize(other.networkBufferSize),
          requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
          networkBuffersPerChannel(other.networkBuffersPerChannel),
          partitionRequestMaxBackoff(other.partitionRequestMaxBackoff),
          partitionRequestInitialBackoff(other.partitionRequestInitialBackoff),
          floatingNetworkBuffersPerGate(other.floatingNetworkBuffersPerGate),
          sortShuffleMinBuffers(other.sortShuffleMinBuffers),
          sortShuffleMinParallelism(other.sortShuffleMinParallelism),
          maxBuffersPerChannel(other.maxBuffersPerChannel)
    {
    }

    OmniShuffleEnvironmentConfiguration& operator=(const OmniShuffleEnvironmentConfiguration& other)
    {
        if (this != &other) {
            numNetworkBuffers = other.numNetworkBuffers;
            networkBufferSize = other.networkBufferSize;
            requestSegmentsTimeoutMillis = other.requestSegmentsTimeoutMillis;
            networkBuffersPerChannel = other.networkBuffersPerChannel;
            partitionRequestMaxBackoff = other.partitionRequestMaxBackoff;
            partitionRequestInitialBackoff = other.partitionRequestInitialBackoff;
            floatingNetworkBuffersPerGate = other.floatingNetworkBuffersPerGate;
            sortShuffleMinBuffers = other.sortShuffleMinBuffers;
            sortShuffleMinParallelism = other.sortShuffleMinParallelism;
            maxBuffersPerChannel = other.maxBuffersPerChannel;
        }
        return *this;
    }

    // Getters
    int getNumNetworkBuffers() const
    {
        return numNetworkBuffers;
    }
    int getNetworkBufferSize() const
    {
        return networkBufferSize;
    }
    long getRequestSegmentsTimeoutMillis() const
    {
        return requestSegmentsTimeoutMillis;
    }
    int getNetworkBuffersPerChannel() const
    {
        return networkBuffersPerChannel;
    }

    int getPartitionRequestMaxBackoff() const
    {
        return partitionRequestMaxBackoff;
    }
    int getFloatingNetworkBuffersPerGate() const
    {
        return floatingNetworkBuffersPerGate;
    }

    int getPartitionRequestInitialBackoff() const
    {
        return partitionRequestInitialBackoff;
    }

    int GetsortShuffleMinBuffers() const
    {
        return sortShuffleMinBuffers;
    }

    int GetsortShuffleMinParallelism() const
    {
        return sortShuffleMinParallelism;
    }
    int GetmaxBuffersPerChannel() const
    {
        return maxBuffersPerChannel;
    }

    // Setters
    void setNumNetworkBuffers(int numNetworkBuffers_)
    {
        this->numNetworkBuffers = numNetworkBuffers_;
    }
    void setNetworkBufferSize(int networkBufferSize_)
    {
        this->networkBufferSize = networkBufferSize_;
    }

    void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis_)
    {
        this->requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis_;
    }
    void setNetworkBuffersPerChannel(int networkBuffersPerChannel_)
    {
        this->networkBuffersPerChannel = networkBuffersPerChannel_;
    }
    void setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff_)
    {
        this->partitionRequestMaxBackoff = partitionRequestMaxBackoff_;
    }

    void setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff_)
    {
        this->partitionRequestInitialBackoff = partitionRequestInitialBackoff_;
    }

    void setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate_)
    {
        this->floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate_;
    }

    void SetsortShuffleMinParallelism(int newsortShuffleMinParallelism)
    {
        this->sortShuffleMinParallelism = newsortShuffleMinParallelism;
    }

    void SetsortShuffleMinBuffers(int newsortShuffleMinBuffers)
    {
        this->sortShuffleMinBuffers = newsortShuffleMinBuffers;
    }

    void SetmaxBuffersPerChannel(int newmaxBuffersPerChannel)
    {
        this->maxBuffersPerChannel = newmaxBuffersPerChannel;
    }

    // toString method
    std::string toString() const
    {
        return "OmniShuffleEnvironmentConfiguration{ numNetworkBuffers=" + std::to_string(numNetworkBuffers) +
               ", networkBufferSize=" + std::to_string(networkBufferSize) +
               ", requestSegmentsTimeoutMillis=" + std::to_string(requestSegmentsTimeoutMillis) +
               ", networkBuffersPerChannel=" + std::to_string(networkBuffersPerChannel) +
               ", partitionRequestInitialBackoff=" + std::to_string(partitionRequestInitialBackoff) +
               ", partitionRequestMaxBackoff=" + std::to_string(partitionRequestMaxBackoff) +
               ", maxBuffersPerChannel=" + std::to_string(maxBuffersPerChannel) + "}";
    };

    static std::shared_ptr<OmniShuffleEnvironmentConfiguration> fromConfiguration(
        int numNetworkBuffers,
        int networkBufferSize,
        long requestSegmentsTimeoutMillis,
        int networkBuffersPerChannel,
        int partitionRequestInitialBackoff,
        int partitionRequestMaxBackoff,
        int floatingNetworkBuffersPerGate,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism,
        int maxBuffersPerChannel);

private:
    int numNetworkBuffers;
    int networkBufferSize;
    long requestSegmentsTimeoutMillis;
    int networkBuffersPerChannel;
    int partitionRequestMaxBackoff;
    int partitionRequestInitialBackoff;
    int floatingNetworkBuffersPerGate;
    int sortShuffleMinBuffers;
    int sortShuffleMinParallelism;
    int maxBuffersPerChannel;
};
} // namespace omnistream

#endif
