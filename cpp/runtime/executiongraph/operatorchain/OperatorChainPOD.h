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

#ifndef OPERATORCHAINPOD_H
#define OPERATORCHAINPOD_H

#include "OperatorPOD.h"

namespace omnistream {
class OperatorChainPOD {
public:
    OperatorChainPOD() : operators()
    {
    }
    explicit OperatorChainPOD(const std::vector<OperatorPOD>& operators) : operators(operators)
    {
    }
    OperatorChainPOD(const OperatorChainPOD& other) = default;
    OperatorChainPOD& operator=(const OperatorChainPOD& other) = default;

    std::vector<OperatorPOD> getOperators() const
    {
        return operators;
    }
    void setOperators(const std::vector<OperatorPOD>& operators_)
    {
        this->operators = operators_;
    }

    std::string toString() const
    {
        std::string operatorsStr = "[";
        for (const auto& op : operators) {
            operatorsStr += op.toString() + ", ";
        }
        if (!operators.empty()) {
            operatorsStr.pop_back(); // Remove the trailing ", "
            operatorsStr.pop_back();
        }
        operatorsStr += "]";
        return "OperatorChainPOD{operators=" + operatorsStr + "}";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(OperatorChainPOD, operators)
private:
    std::vector<OperatorPOD> operators;
};

} // namespace omnistream

#endif
