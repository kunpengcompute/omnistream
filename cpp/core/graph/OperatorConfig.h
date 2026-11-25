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

#ifndef FLINK_TNEL_OPERATORCONFIG_H
#define FLINK_TNEL_OPERATORCONFIG_H

#include <string>
#include <nlohmann/json.hpp>

/**  row type example
 *  [
                {
                    "kind": "Row",
                    "type": [
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "precision": 3,
                            "timestampKind": 0,
                            "type": "TIMESTAMP"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "length": 2147483647,
                            "type": "VARCHAR"
                        }
                    ]
                }
            ],
 */

/***
 *   basic type example
 *       {
                            "kind": "basic",
                            "type": "String"
          }
 *
 */
namespace omnistream {
class OperatorConfig {
public:
    OperatorConfig(
        std::string uniqueName,
        std::string name,
        nlohmann::json inputType,
        nlohmann::json outputType,
        nlohmann::json description);
    OperatorConfig() = default;

    std::string getUniqueName() const;

    nlohmann::json getInputType() const;

    nlohmann::json getOutputType() const;

    std::string getUdfName() const;

    std::string getName() const;

    nlohmann::json getUdfInputType() const;

    nlohmann::json getUdfOutputType() const;

    nlohmann::json getDescription() const;

    void setUniqueName(std::string uniqueName)
    {
        uniqueName_ = uniqueName;
    }

    void setDescription(nlohmann::json description)
    {
        uniqueName_ = description;
    }

private:
    std::string uniqueName_;
    std::string name_;
    nlohmann::json inputType_;
    nlohmann::json outputType_;

    std::string udfName_;
    nlohmann::json udfInputType_;
    nlohmann::json udfOutputType_;

    nlohmann::json description_;
};
} // namespace omnistream

#endif
