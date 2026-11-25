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

#include "temp_batch_function.h"

namespace omnistream {
// for DATE_FORMAT
BaseVector* BatchDateFormat(BaseVector* inputVec, const std::string& format)
{
    if (inputVec->GetEncoding() != OMNI_FLAT) {
        throw std::runtime_error("Only flat encoding is supported for DATE_FORMAT function.");
    }

    if (format != "yyyy-MM-dd" && format != "HH:mm") {
        throw std::runtime_error("Unsupported format string for DATE_FORMAT function.");
    }
    auto input = reinterpret_cast<Vector<int64_t>*>(inputVec);
    auto newVec = new Vector<LargeStringContainer<std::string_view>>(input->GetSize());
    if (format == "yyyy-MM-dd") {
        char buffer[11]; // "yyyy-MM-dd" + null terminator
        std::string_view sv(buffer, 10);
        for (int i = 0; i < input->GetSize(); i++) {
            if (UNLIKELY(input->IsNull(i))) {
                newVec->SetNull(i);
            } else {
                std::tm timeStruct{};
                time_t timeVal = static_cast<time_t>(input->GetValue(i) / 1000);
                // Convert epoch to struct tm (UTC)
                gmtime_r(&timeVal, &timeStruct);
                strftime(buffer, sizeof(buffer), "%Y-%m-%d", &timeStruct);
                newVec->SetValue(i, sv);
            }
        }
    } else if (format == "HH:mm") {
        char buffer2[6]; // "yyyy-MM-dd" + null terminator
        std::string_view sv2(buffer2, 5);
        for (int i = 0; i < input->GetSize(); i++) {
            if (UNLIKELY(input->IsNull(i))) {
                newVec->SetNull(i);
            } else {
                std::tm timeStruct{};
                time_t timeVal = static_cast<time_t>(input->GetValue(i));
                // Convert epoch to struct tm (UTC)
                gmtime_r(&timeVal, &timeStruct);
                strftime(buffer2, sizeof(buffer2), "%H:%M", &timeStruct);
                newVec->SetValue(i, sv2);
            }
        }
    } else {
        NOT_IMPL_EXCEPTION;
    }
    return newVec;
}

BaseVector* RegexpExtract(BaseVector* inputVec, std::string& regexToMatch, int32_t group)
{
    auto returnVec = new Vector<LargeStringContainer<std::string_view>>(inputVec->GetSize());
    std::regex re(regexToMatch, std::regex_constants::optimize);
    std::match_results<std::string_view::const_iterator> match;
    if (inputVec->GetEncoding() == OMNI_FLAT) {
        auto castedVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(inputVec);
        for (int i = 0; i < castedVec->GetSize(); i++) {
            if (castedVec->IsNull(i)) {
                returnVec->SetNull(i);
                continue;
            }
            auto val = castedVec->GetValue(i);
            if (std::regex_search(val.begin(), val.end(), match, re) && group >= 0 &&
                static_cast<size_t>(group) < match.size() && match[group].matched) {
                std::string_view sv(&(*match[group].first), std::distance(match[group].first, match[group].second));
                returnVec->SetValue(i, sv);
            } else {
                returnVec->SetNull(i);
            }
        }
    } else if (inputVec->GetEncoding() == OMNI_DICTIONARY) {
        auto castedVec = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
            inputVec);
        for (int i = 0; i < castedVec->GetSize(); i++) {
            if (castedVec->IsNull(i)) {
                returnVec->SetNull(i);
                continue;
            }
            auto val = castedVec->GetValue(i);
            if (std::regex_search(val.begin(), val.end(), match, re) && group >= 0 &&
                static_cast<size_t>(group) < match.size() && match[group].matched) {
                std::string_view sv(&(*match[group].first), std::distance(match[group].first, match[group].second));
                returnVec->SetValue(i, sv);
            } else {
                returnVec->SetNull(i);
            }
        }
    }
    return returnVec;
}
} // namespace omnistream
