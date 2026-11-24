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

#ifndef OMNISTREAM_DUMMYSPLITFETCHERTASK_H
#define OMNISTREAM_DUMMYSPLITFETCHERTASK_H

#include "SplitFetcherTask.h"

class DummySplitFetcherTask : public SplitFetcherTask {
public:
    explicit DummySplitFetcherTask(std::string name) : name(name)
    {
    }

    bool Run() override
    {
        return false;
    }

    void WakeUp() override
    {
    }

    std::string ToString() override
    {
        return name;
    }

private:
    std::string name;
};

#endif // OMNISTREAM_DUMMYSPLITFETCHERTASK_H
