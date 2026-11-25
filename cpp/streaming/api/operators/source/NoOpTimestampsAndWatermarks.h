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

#ifndef OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H
#define OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H

#include "TimestampsAndWatermarks.h"

class NoOpTimestampsAndWatermarks : public TimestampsAndWatermarks {
public:
    ReaderOutput* CreateMainOutput(OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback) override;

    explicit NoOpTimestampsAndWatermarks(TimestampAssigner* timestamps);

    ~NoOpTimestampsAndWatermarks() override
    {
        delete timestampAssigner;
    }

private:
    TimestampAssigner* timestampAssigner;

    class TimestampsOnlyOutput : public ReaderOutput {
    public:
        TimestampsOnlyOutput(OmniDataOutputPtr output, TimestampAssigner* timestampAssigner);

        ~TimestampsOnlyOutput() override
        {
            delete reusingRecord;
        }

        void Collect(void* record) override;
        void Collect(void* record, long timestamp) override;
        SourceOutput& CreateOutputForSplit(const std::string& splitId) override;
        void ReleaseOutputForSplit(const std::string& splitId) override;

    private:
        OmniDataOutputPtr output;
        TimestampAssigner* timestampAssigner;
        StreamRecord* reusingRecord;
    };
};

#endif // OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H
