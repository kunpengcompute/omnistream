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

#include "../../core/memory/DataInputView.h"
#include "TimeWindow.h"

TimeWindow::TimeWindow()
{
}

TimeWindow::TimeWindow(int64_t start, int64_t end) : start(start), end(end)
{
}

TimeWindow::Serializer::Serializer()
{
}

bool TimeWindow::Serializer::isImmutableType() const
{
    return true;
}

TimeWindow* TimeWindow::Serializer::createInstance() const
{
    return new TimeWindow(0, 1);
}

TimeWindow* TimeWindow::Serializer::copy(TimeWindow* from) const
{
    return from;
}

TimeWindow* TimeWindow::Serializer::copy(TimeWindow* from, TimeWindow* reuse) const
{
    return from;
}

int32_t TimeWindow::Serializer::getLength() const
{
    return sizeof(int64_t) * 2;
}

void TimeWindow::Serializer::serialize(void* input, DataOutputSerializer& target)
{
    TimeWindow* record = (TimeWindow*)input;
    target.writeLong(record->start);
    target.writeLong(record->end);
}

void* TimeWindow::Serializer::deserialize(DataInputView& source)
{
    int64_t start1;
    int64_t end1;
    start1 = source.readLong();
    end1 = source.readLong();
    return new TimeWindow(start1, end1);
}

void TimeWindow::Serializer::copy(DataInputView* source, DataOutputSerializer* target) const
{
    target->writeLong(source->readLong());
    target->writeLong(source->readLong());
}
