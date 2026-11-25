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

#ifndef ABSTRACT_EVENT_H
#define ABSTRACT_EVENT_H

#include <common.h>

namespace omnistream {
class AbstractEvent {
public:
    static const int abstractEvent;
    static const int endOfPartition;
    static const int endOfData;
    static const int checkpointbarrier;

    virtual int GetEventClassID()
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual std::string GetEventClassName()
    {
        NOT_IMPL_EXCEPTION;
    }
};
} // namespace omnistream

#endif
