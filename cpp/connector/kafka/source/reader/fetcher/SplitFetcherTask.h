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

#ifndef OMNISTREAM_SPLITFETCHERTASK_H
#define OMNISTREAM_SPLITFETCHERTASK_H

#include <string>

class SplitFetcherTask {
public:
    virtual bool Run() = 0;
    virtual void WakeUp() = 0;
    virtual std::string ToString() = 0;
    virtual ~SplitFetcherTask() = default;
};

#endif // OMNISTREAM_SPLITFETCHERTASK_H
