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

#ifndef FLINK_TNEL_COMMITTABLEMESSAGESERIALIZER_H
#define FLINK_TNEL_COMMITTABLEMESSAGESERIALIZER_H

#include "../../core/typeutils/TypeSerializer.h"
class CommittableMessageSerializer : public TypeSerializer {
    const char* getName() const override
    {
        return "CommittableMessageSerializer";
    }

    void* deserialize(DataInputView& source)
    {
        return nullptr;
    }
    void serialize(void* record, DataOutputSerializer& target)
    {
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BIGINT_BK;
    };
};
#endif
