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

#ifndef OPERATORPOD_H
#define OPERATORPOD_H

#include <string>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include "TypeDescriptionPOD.h" // Include for TypeDescriptionPOD

namespace omnistream {
enum Type_o {
    INVALID = 0,
    SQL = 1,
    STREAM = 2,
    SQL_STREAM = 3
};

class OperatorPOD {
public:
    OperatorPOD()
        : name(""),
          id(""),
          description(""),
          inputs(),
          output(),
          operatorId(""),
          vertexID(),
          jobType(Type_o::INVALID),
          taskType(Type_o::INVALID),
          operatorType(Type_o::INVALID) {};

    OperatorPOD(
        const std::string& name,
        const std::string& id,
        const std::string& description,
        const std::vector<TypeDescriptionPOD>& inputs,
        const TypeDescriptionPOD& output)
        : OperatorPOD(name, id, description, inputs, output, "", 0, Type_o::INVALID, Type_o::INVALID, Type_o::INVALID)
    {
    }

    OperatorPOD(
        const std::string& name,
        const std::string& id,
        const std::string& description,
        const std::vector<TypeDescriptionPOD>& inputs,
        const TypeDescriptionPOD& output,
        const std::string& operatorId,
        const int vertexID,
        const int jobType,
        const int taskType,
        const int operatorType)
        : name(name),
          id(id),
          description(description),
          inputs(inputs),
          output(output),
          operatorId(operatorId),
          vertexID(vertexID),
          jobType(jobType),
          taskType(taskType),
          operatorType(operatorType)
    {
    }

    OperatorPOD(const OperatorPOD& other) = default;
    OperatorPOD& operator=(const OperatorPOD& other) = default;

    std::string getName() const
    {
        return name;
    }
    void setName(const std::string& name_)
    {
        this->name = name_;
    }
    std::string getId() const
    {
        return id;
    }
    void setId(const std::string& id_)
    {
        this->id = id_;
    }
    std::string getDescription() const
    {
        return description;
    }
    void setDescription(const std::string& description_)
    {
        this->description = description_;
    }
    const std::vector<TypeDescriptionPOD>& getInputs() const
    {
        return inputs;
    }
    void setInputs(const std::vector<TypeDescriptionPOD>& inputs_)
    {
        this->inputs = inputs_;
    }
    const TypeDescriptionPOD& getOutput() const
    {
        return output;
    }
    void setOutput(const TypeDescriptionPOD& output_)
    {
        this->output = output_;
    }
    const int getVertexID() const
    {
        return vertexID;
    }
    void setVertexID(const int vertexID_)
    {
        this->vertexID = vertexID_;
    }
    std::string getOperatorId() const
    {
        return operatorId;
    }
    void setOperatorId(const std::string& operatorId_)
    {
        this->operatorId = operatorId_;
    }
    const int getJobType() const
    {
        return jobType;
    }
    void setJobType(const Type_o jobType)
    {
        this->jobType = jobType;
    }
    const int getTaskType() const
    {
        return taskType;
    }
    void setTaskType(const Type_o taskType)
    {
        this->taskType = taskType;
    }
    const int getOperatorType() const
    {
        return operatorType;
    }
    void setVOperatorType(const Type_o operatorType)
    {
        this->operatorType = operatorType;
    }

    bool operator==(const OperatorPOD& other) const
    {
        return this->name == other.getName() && this->id == other.getId() && this->description == other.description &&
               this->inputs == other.inputs && this->output == other.output && this->vertexID == other.vertexID &&
               this->jobType == other.jobType && this->taskType == other.taskType &&
               this->operatorType == other.operatorType;
    }

    std::string toString() const
    {
        std::string inputsStr = "[";
        for (const auto& input : inputs) {
            inputsStr += input.toString() + ", ";
        }
        if (!inputs.empty()) {
            inputsStr.pop_back(); // Remove last comma and space
            inputsStr.pop_back();
        }
        inputsStr += "]";
        return "OperatorPOD{name=" + name + ", id=" + id + ", description=" + description + ", inputs=" + inputsStr +
               ", output=" + output.toString() + ", operatorId=" + operatorId +
               ", vertexID=" + std::to_string(vertexID) + ", jobType=" + std::to_string(jobType) +
               ", taskType=" + std::to_string(taskType) + ", operatorType=" + std::to_string(operatorType) + "}";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        OperatorPOD, name, id, description, inputs, output, operatorId, vertexID, jobType, taskType, operatorType)

private:
    std::string name;
    std::string id;
    std::string description;
    std::vector<TypeDescriptionPOD> inputs;
    TypeDescriptionPOD output;
    std::string operatorId;
    int vertexID;
    int jobType;      // enum 0(NULL), 1(SQL), 2(STREAM), 3(SQL_STREAM)
    int taskType;     // enum 0(NULL), 1(SQL), 2(STREAM), 3(SQL_STREAM)
    int operatorType; // enum 0(NULL), 1(SQL), 2(STREAM)
};

} // namespace omnistream
namespace std {
template <>
struct hash<omnistream::OperatorPOD> {
    size_t operator()(const omnistream::OperatorPOD& OperatorPOD) const
    {
        size_t seed = 0;

        std::size_t h1 = std::hash<std::string>()(OperatorPOD.getId());
        std::size_t h2 = std::hash<std::string>()(OperatorPOD.getName());
        std::size_t h3 = std::hash<std::string>()(OperatorPOD.getDescription());
        std::vector<omnistream::TypeDescriptionPOD> v1 = OperatorPOD.getInputs();

        for (const auto& elem : v1) {
            seed ^= std::hash<omnistream::TypeDescriptionPOD>{}(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        std::size_t h5 = std::hash<omnistream::TypeDescriptionPOD>()(OperatorPOD.getOutput());
        seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

        return seed;
    }
};
} // namespace std

#endif // OPERATORPOD_H
