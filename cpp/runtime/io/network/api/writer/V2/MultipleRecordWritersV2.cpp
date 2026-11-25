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

#include "MultipleRecordWritersV2.h"
namespace omnistream {

MultipleRecordWritersV2::MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters)
{
    this->recordWriters.reserve(recordWriters.size());
    for (auto* writer : recordWriters) {
        this->recordWriters.emplace_back(writer);
    }
}

RecordWriterV2* MultipleRecordWritersV2::getRecordWriter(int outputIndex)
{
    // Observer only - ownership stays with this delegate.
    return recordWriters[outputIndex].get();
}

void MultipleRecordWritersV2::close()
{
    LOG_INFO_IMP("MultipleRecordWritersV2 close");
    for (auto& writer : recordWriters) {
        writer->close();
    }
}

void MultipleRecordWritersV2::cancel()
{
    LOG_INFO_IMP("MultipleRecordWritersV2 cancel");
    for (auto& writer : recordWriters) {
        writer->cancel();
    }
}
} // namespace omnistream
