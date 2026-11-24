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

#ifndef FLINK_TNEL_COMMITTABLEMESSAGEINFO_H
#define FLINK_TNEL_COMMITTABLEMESSAGEINFO_H

#include "TypeInformation.h"
#include "core/typeutils/CommittableMessageSerializer.h"

class CommittableMessageInfo : public TypeInformation {
public:
    explicit CommittableMessageInfo()
    {
        typeSerializer = new CommittableMessageSerializer();
    };

    ~CommittableMessageInfo() override
    {
        delete typeSerializer;
    }

    TypeSerializer* createTypeSerializer() override
    {
        return typeSerializer;
    };

    std::string name() override
    {
        return typeSerializer->getName();
    };

    BackendDataType getBackendId() const override
    {
        return typeSerializer->getBackendId();
    }

private:
    TypeSerializer* typeSerializer;
};

#endif
