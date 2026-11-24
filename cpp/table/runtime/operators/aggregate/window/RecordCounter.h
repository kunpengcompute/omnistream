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

#ifndef FLINK_TNEL_RECORDCOUNTER_H
#define FLINK_TNEL_RECORDCOUNTER_H

#include "table/data/RowData.h"

class RecordCounter {
public:
    virtual ~RecordCounter() = default;
    virtual bool recordCountIsZero(RowData* acc) = 0;

    static std::unique_ptr<RecordCounter> of(int indexOfCountStar_);

protected:
    RecordCounter() = default;
};

class AccumulationRecordCounter : public RecordCounter {
public:
    bool recordCountIsZero(RowData* acc) override;
};

class RetractionRecordCounter : public RecordCounter {
public:
    explicit RetractionRecordCounter(int indexOfCountStar_);

    bool recordCountIsZero(RowData* acc) override;

private:
    int indexOfCountStar;
};

#endif // FLINK_TNEL_RECORDCOUNTER_H
