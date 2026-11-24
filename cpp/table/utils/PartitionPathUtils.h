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

#ifndef PARTITION_PATH_UTILS_H
#define PARTITION_PATH_UTILS_H

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <utility>

class PartitionPathUtils {
public:
    static std::string generatePartitionPath(const std::vector<std::pair<std::string, std::string>>& partitionSpec)
    {
        if (partitionSpec.empty()) {
            return "";
        }

        std::ostringstream suffixBuf;
        for (size_t i = 0; i < partitionSpec.size(); ++i) {
            if (i > 0) {
                suffixBuf << "/";
            }
            const auto& [key, value] = partitionSpec[i];
            suffixBuf << escapePathName(key) << "=" << escapePathName(value);
        }
        suffixBuf << "/";
        return suffixBuf.str();
    }

    static std::string escapePathName(const std::string& path)
    {
        if (path.empty()) {
            throw std::invalid_argument("Path should not be empty");
        }

        std::ostringstream sb;
        for (char c : path) {
            if (needsEscaping(c)) {
                escapeChar(c, sb);
            } else {
                sb << c;
            }
        }
        return sb.str();
    }

private:
    static bool needsEscaping(char c)
    {
        static const std::string escapeChars = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"
                                               "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F"
                                               "\"#%'*/:=?\\\x7F{}[]^";
        return escapeChars.find(c) != std::string::npos || c < ' ';
    }

    static void escapeChar(char c, std::ostringstream& sb)
    {
        sb << '%' << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
           << static_cast<int>(static_cast<unsigned char>(c));
    }
};

#endif // PARTITION_PATH_UTILS_H
