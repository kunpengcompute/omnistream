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

#include <gtest/gtest.h>
#include "globals.h"

long vecBatchRows = 1000;
bool isFlush = false;

static void HandleCustomArgs(int argc, char** argv)
{
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--VectorBatch_rows") {
            if (i + 1 < argc) {
                char* endptr;
                vecBatchRows = std::strtol(argv[++i], &endptr, 10);
                std::cout << "vecBatchRows: " << vecBatchRows << std::endl;
            }
        }
        if (std::string(argv[i]) == "--flush") {
            if (i + 1 < argc) {
                char* endptr;
                isFlush = std::strtol(argv[++i], &endptr, 10);
                std::cout << "isFlush: " << isFlush << std::endl;
            } else {
                std::cerr << "--input_file requires a filename argument." << std::endl;
                exit(1);
            }
        }
    }
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    HandleCustomArgs(argc, argv);
    return RUN_ALL_TESTS();
}
