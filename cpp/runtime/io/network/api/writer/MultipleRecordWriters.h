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

#ifndef OMNISTREAM_MULTIRECORDWRITERS_H
#define OMNISTREAM_MULTIRECORDWRITERS_H

#include "RecordWriterDelegate.h"

namespace omnistream::datastream {
class MultipleRecordWriters : public RecordWriterDelegate {
public:
    explicit MultipleRecordWriters(std::vector<RecordWriter*>& recordWriters);

    RecordWriter* getRecordWriter(int outputIndex) override;

    ~MultipleRecordWriters() override = default;

private:
    std::vector<RecordWriter*> recordWriters;
};
} // namespace omnistream::datastream

#endif
