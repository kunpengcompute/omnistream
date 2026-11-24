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

#ifndef STREAMCONFIGPOD_H
#define STREAMCONFIGPOD_H

#include <vector>
#include <string>
#include "StreamEdgePOD.h" // Assuming StreamEdgePOD.h exists
#include "NonChainedOutputPOD.h"

#include <nlohmann/json.hpp>

#include "operatorchain/OperatorPOD.h"

namespace omnistream {
class StreamConfigPOD {
public:
    StreamConfigPOD() = default;

    StreamConfigPOD(
        const std::vector<StreamEdgePOD>& outEdgesInOrder,
        const std::string& stateBackend,
        const std::vector<NonChainedOutputPOD>& opNonChainedOutputs,
        const std::vector<StreamEdgePOD>& opChainedOutputs,
        const OperatorPOD& operatorDescription,
        const std::string& operatorFactoryName,
        const std::vector<StreamEdgePOD>& inStreamEdges,
        int numberOfNetworkInputs,
        const std::unordered_map<std::string, std::string>& omniConf)
        : outEdgesInOrder(outEdgesInOrder),
          stateBackend(stateBackend),
          opNonChainedOutputs(opNonChainedOutputs),
          opChainedOutputs(opChainedOutputs),
          operatorDescription(operatorDescription),
          operatorFactoryName(operatorFactoryName),
          inStreamEdges(inStreamEdges),
          numberOfNetworkInputs(numberOfNetworkInputs),
          omniConf(omniConf)
    {
    }

    StreamConfigPOD(const StreamConfigPOD& other)
        : outEdgesInOrder(other.outEdgesInOrder),
          opNonChainedOutputs(other.opNonChainedOutputs),
          opChainedOutputs(other.opChainedOutputs),
          operatorDescription(other.operatorDescription),
          operatorFactoryName(other.operatorFactoryName),
          inStreamEdges(other.inStreamEdges),
          numberOfNetworkInputs(other.numberOfNetworkInputs),
          omniConf(other.omniConf)
    {
    }

    StreamConfigPOD(StreamConfigPOD&& other) noexcept
        : outEdgesInOrder(std::move(other.outEdgesInOrder)),
          opNonChainedOutputs(std::move(other.opNonChainedOutputs)),
          opChainedOutputs(std::move(other.opChainedOutputs)),
          operatorDescription(std::move(other.operatorDescription)),
          operatorFactoryName(std::move(other.operatorFactoryName)),
          inStreamEdges(std::move(other.inStreamEdges)),
          numberOfNetworkInputs(std::move(other.numberOfNetworkInputs)),
          omniConf(std::move(other.omniConf))
    {
    }

    StreamConfigPOD& operator=(const StreamConfigPOD& other)
    {
        if (this != &other) {
            outEdgesInOrder = other.outEdgesInOrder;
            opNonChainedOutputs = other.opNonChainedOutputs;
            opChainedOutputs = other.opChainedOutputs;
            operatorDescription = other.operatorDescription;
            operatorFactoryName = other.operatorFactoryName;
            inStreamEdges = other.inStreamEdges;
            numberOfNetworkInputs = other.numberOfNetworkInputs;
            omniConf = other.omniConf;
        }
        return *this;
    }

    StreamConfigPOD& operator=(StreamConfigPOD&& other) noexcept
    {
        if (this != &other) {
            outEdgesInOrder = std::move(other.outEdgesInOrder);
            opNonChainedOutputs = std::move(other.opNonChainedOutputs);
            opChainedOutputs = std::move(other.opChainedOutputs);
            operatorDescription = std::move(other.operatorDescription);
            operatorFactoryName = std::move(other.operatorFactoryName);
            inStreamEdges = std::move(other.inStreamEdges);
            numberOfNetworkInputs = std::move(other.numberOfNetworkInputs);
            omniConf = std::move(other.omniConf);
        }
        return *this;
    }

    bool operator==(const StreamConfigPOD& other) const
    {
        bool result1 = outEdgesInOrder == other.outEdgesInOrder;
        bool result2 = opNonChainedOutputs == other.opNonChainedOutputs;
        bool result3 = opChainedOutputs == other.opChainedOutputs;
        bool result5 = operatorDescription == other.operatorDescription;
        bool result4 = operatorFactoryName == other.operatorFactoryName;

        return result1 && result2 && result3 && result4 && result5;
    }

    ~StreamConfigPOD() = default;

    std::vector<StreamEdgePOD> getOutEdgesInOrder() const
    {
        return outEdgesInOrder;
    }

    void setOutEdgesInOrder(const std::vector<StreamEdgePOD>& outEdgesInOrder_)
    {
        this->outEdgesInOrder = outEdgesInOrder_;
    }

    std::vector<NonChainedOutputPOD> getOpNonChainedOutputs() const
    {
        return opNonChainedOutputs;
    }

    void setOpNonChainedOutputs(const std::vector<NonChainedOutputPOD>& opNonChainedOutputs_)
    {
        this->opNonChainedOutputs = opNonChainedOutputs_;
    }

    std::vector<StreamEdgePOD> getOpChainedOutputs() const
    {
        return opChainedOutputs;
    }

    void setOpChainedOutputs(const std::vector<StreamEdgePOD>& opChainedOutputs_)
    {
        this->opChainedOutputs = opChainedOutputs_;
    }

    OperatorPOD getOperatorDescription() const
    {
        return operatorDescription;
    }

    void setOperatorDescription(const OperatorPOD& operatorDescription_)
    {
        this->operatorDescription = operatorDescription_;
    }

    std::string getOperatorFactoryName() const
    {
        return operatorFactoryName;
    }

    void setOperatorFactoryName(const std::string& operatorFactoryName_)
    {
        this->operatorFactoryName = operatorFactoryName_;
    }

    std::string getStateBackend() const
    {
        return stateBackend;
    }

    void setStateBackend(const std::string& stateBackend_)
    {
        this->stateBackend = stateBackend_;
    }

    std::vector<StreamEdgePOD> getInStreamEdges() const
    {
        return inStreamEdges;
    }

    void setInStreamEdges(const std::vector<StreamEdgePOD>& inStreamEdges_)
    {
        this->inStreamEdges = inStreamEdges_;
    }

    int getNumberOfNetworkInputs() const
    {
        return numberOfNetworkInputs;
    }

    void setNumberOfNetworkInputs(const int numberOfNetworkInputs_)
    {
        this->numberOfNetworkInputs = numberOfNetworkInputs_;
    }

    std::unordered_map<std::string, std::string> getOmniConf() const
    {
        return omniConf;
    }

    std::string toString() const
    {
        std::string result = "StreamConfigPOD{  outEdgesInOrder =[";
        for (size_t i = 0; i < outEdgesInOrder.size(); ++i) {
            result += outEdgesInOrder[i].toString();
            if (i < outEdgesInOrder.size() - 1) {
                result += ", ";
            }
        }
        result += "], opNonChainedOutputs=[";
        for (size_t i = 0; i < opNonChainedOutputs.size(); ++i) {
            result += opNonChainedOutputs[i].toString();
            if (i < opNonChainedOutputs.size() - 1) {
                result += ", ";
            }
        }

        result += "], opChainedOutputs=[";
        for (size_t i = 0; i < opChainedOutputs.size(); ++i) {
            result += opChainedOutputs[i].toString();
            if (i < opChainedOutputs.size() - 1) {
                result += ", ";
            }
        }

        result += "], inStreamEdges=[";
        for (size_t i = 0; i < inStreamEdges.size(); ++i) {
            result += inStreamEdges[i].toString();
            if (i < inStreamEdges.size() - 1) {
                result += ", ";
            }
        }

        result += "], operatorDescription=" + operatorDescription.toString() + ", operatorFactoryName=\"" +
                  operatorFactoryName + ", numberOfNetworkInputs=\"" + std::to_string(numberOfNetworkInputs);

        result += "], omniConf={";
        for (const auto& [key, value] : omniConf) {
            result += "[" + key + ", " + value + "] ";
        }
        result += "}";
        result += "\"}";

        return result;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        StreamConfigPOD,
        outEdgesInOrder,
        opNonChainedOutputs,
        opChainedOutputs,
        operatorDescription,
        operatorFactoryName,
        inStreamEdges,
        numberOfNetworkInputs,
        omniConf)
private:
    std::vector<StreamEdgePOD> outEdgesInOrder;
    std::string stateBackend;

    std::vector<NonChainedOutputPOD> opNonChainedOutputs;
    std::vector<StreamEdgePOD> opChainedOutputs;

    OperatorPOD operatorDescription;

    std::string operatorFactoryName;
    std::vector<StreamEdgePOD> inStreamEdges;
    int numberOfNetworkInputs;
    std::unordered_map<std::string, std::string> omniConf;
};
} // namespace omnistream
namespace std {
template <>
struct hash<omnistream::StreamConfigPOD> {
    size_t operator()(const omnistream::StreamConfigPOD& pod) const
    {
        size_t seed = 0;

        auto hash_combine = [&seed](size_t h) { seed ^= h + 0x9e3779b9 + (seed << 6) + (seed >> 2); };

        for (const auto& edge : pod.getOutEdgesInOrder()) {
            hash_combine(std::hash<omnistream::StreamEdgePOD>{}(edge));
        }

        for (const auto& output : pod.getOpNonChainedOutputs()) {
            hash_combine(std::hash<omnistream::NonChainedOutputPOD>{}(output));
        }

        for (const auto& edge : pod.getOpChainedOutputs()) {
            hash_combine(std::hash<omnistream::StreamEdgePOD>{}(edge));
        }

        hash_combine(std::hash<omnistream::OperatorPOD>{}(pod.getOperatorDescription()));
        hash_combine(std::hash<std::string>{}(pod.getOperatorFactoryName()));
        hash_combine(std::hash<std::string>{}(pod.getStateBackend()));

        return seed;
    }
};
} // namespace std

#endif
