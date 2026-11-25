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

#include "KeyFieldInfoPOD.h"

#include <sstream>

namespace omnistream {

KeyFieldInfoPOD::KeyFieldInfoPOD() : fieldIndex(0)
{
}

KeyFieldInfoPOD::KeyFieldInfoPOD(const std::string& fieldName, const std::string& fieldTypeName, int fieldIndex)
    : fieldName(fieldName),
      fieldTypeName(fieldTypeName),
      fieldIndex(fieldIndex)
{
}

std::string KeyFieldInfoPOD::getFieldName() const
{
    return fieldName;
}

std::string KeyFieldInfoPOD::getFieldTypeName() const
{
    return fieldTypeName;
}

int KeyFieldInfoPOD::getFieldIndex() const
{
    return fieldIndex;
}

void KeyFieldInfoPOD::setFieldName(const std::string& fieldName_)
{
    this->fieldName = fieldName_;
}

void KeyFieldInfoPOD::setFieldTypeName(const std::string& fieldTypeName_)
{
    this->fieldTypeName = fieldTypeName_;
}

void KeyFieldInfoPOD::setFieldIndex(int fieldIndex_)
{
    this->fieldIndex = fieldIndex_;
}

std::string KeyFieldInfoPOD::toString() const
{
    std::stringstream ss;
    ss << "FieldInfo{"
       << "fieldName='" << fieldName << '\'' << ", fieldTypeName='" << fieldTypeName << '\''
       << ", fieldIndex=" << fieldIndex << '}';
    return ss.str();
}

} // namespace omnistream

/**
 *


// main.cpp (Example Usage)
#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>
#include "keyfieldinfopojo.h"

int main() {
    demo::KeyFieldInfoPOJO myField("exampleField", "java.lang.String", 0);

    // Serialization to JSON string
    nlohmann::json j = myField; // Implicitly uses the intrusive serialization
    std::string jsonString = j.dump(4);

    std::cout << "JSON string:\n" << jsonString << std::endl;

    // Serialization to JSON file
    std::ofstream outfile("myfield.json");
    outfile << j;
    outfile.close();

    // Deserialization from JSON string
    demo::KeyFieldInfoPOJO myField2 = nlohmann::json::parse(jsonString); // Implicitly uses the intrusive
deserialization

    std::cout << "\nDeserialized object:\n" << myField2.toString() << std::endl;


     // Deserialization from JSON file
    std::ifstream infile("myfield.json");
    nlohmann::json j3;
    infile >> j3;
    infile.close();
    demo::KeyFieldInfoPOJO myField3 = j3; // Implicitly uses the intrusive deserialization
    std::cout << "\nDeserialized from file:\n" << myField3.toString() << std::endl;

    return 0;
}
 */
