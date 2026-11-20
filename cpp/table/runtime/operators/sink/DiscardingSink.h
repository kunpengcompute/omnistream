/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_DISCARDINGSINK_H
#define FLINK_TNEL_DISCARDINGSINK_H

#include <regex>
#include "SinkFunction.h"

class DiscardingSink : public SinkFunction<StreamRecord*> {
public:
    explicit DiscardingSink(const nlohmann::json& sinkDescription)
        : outfile(sinkDescription.contains("outputfile") ? sinkDescription["outputfile"] : "")
    {
        if (sinkDescription.contains("inputTypes")) {
            std::regex pattern(R"(DECIMAL\d+\((\d+),\s*(\d+)\))");
            std::smatch match;

            inputTypes = sinkDescription["inputTypes"].get<std::vector<std::string>>();
            for (const std::string& inputType : inputTypes) {
                if (std::regex_search(inputType, match, pattern)) {
                    int precision = std::stoi(match[1].str());
                    int scale = std::stoi(match[2].str());
                    decimalInfo.emplace_back(precision, scale);
                } else {
                    decimalInfo.emplace_back(-1, -1);
                }
            }
        }
        if (sinkDescription.contains("timeZone")) {
            // The session zone (table.local-time-zone) is resolved on the Java side,
            // defaulting to ZoneId.systemDefault(). An empty value means "use the
            // native OS local time", matching Flink's systemDefault fallback.
            timeZone = sinkDescription.value("timeZone", "");
        }
    };

    ~DiscardingSink() override = default;

    void invoke(StreamRecord* data, SinkInputValueType valueType) override
    {
        if (valueType == SinkInputValueType::ROW_DATA) {
        } else if (valueType == SinkInputValueType::VEC_BATCH) {
            if (outfile == "") {
                delete reinterpret_cast<omnistream::VectorBatch*>(data->getValue());
                delete data;
                return;
            }
            auto vb = reinterpret_cast<omnistream::VectorBatch*>(data->getValue());
            vb->writeToFile(outfile, std::ios::app, decimalInfo, inputTypes, timeZone);

            delete static_cast<omnistream::VectorBatch*>(data->getValue());
            delete data;
        }
    };
    void writeWatermark(Watermark* watermark) override
    {
    }
    void finish() override {};

private:
    std::vector<std::string> inputTypes;
    std::string outfile;
    std::vector<std::pair<int32_t, int32_t>> decimalInfo; // precision, scale
    std::string timeZone;
};

#endif // FLINK_TNEL_DISCARDINGSINK_H
