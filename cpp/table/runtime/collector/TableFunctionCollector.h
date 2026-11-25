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

#ifndef FLINK_TNEL_TABLEFUNCTIONCOLLECTOR_H
#define FLINK_TNEL_TABLEFUNCTIONCOLLECTOR_H

#include "functions/Collector.h"

class TableFunctionCollector : public Collector {
public:
    void setInput(void* input_)
    {
        this->input = input_;
    }
    void* getInput()
    {
        return input;
    }
    void reset();
    void outputResult(void* result);
    bool isCollected() const
    {
        return collected;
    }
    void close() override
    {
        this->collector->close();
    }
    void setCollector(Collector* collector_)
    {
        this->collector = collector_;
    }
    void collect(void* result) override
    {
        collector->collect(result);
        collected = true;
    }

private:
    bool collected;
    void* input;
    // The downstream
    Collector* collector;
};

#endif
