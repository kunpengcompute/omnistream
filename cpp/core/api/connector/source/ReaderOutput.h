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

#ifndef FLINK_TNEL_READEROUTPUT_H
#define FLINK_TNEL_READEROUTPUT_H

#include <memory>
#include <string>
#include "SourceOutput.h"

class ReaderOutput : public SourceOutput {
public:
    virtual SourceOutput& CreateOutputForSplit(const std::string& splitId) = 0;

    virtual void ReleaseOutputForSplit(const std::string& splitId) = 0;
};

#endif // FLINK_TNEL_READEROUTPUT_H
