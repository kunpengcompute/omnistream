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

#ifndef FLINK_TNEL_REUSINGDESERIALIZATIONDELEGATE_H
#define FLINK_TNEL_REUSINGDESERIALIZATIONDELEGATE_H

#include "DeserializationDelegate.h"
#include "core/typeutils/TypeSerializer.h"

class ReusingDeserializationDelegate : public DeserializationDelegate {
public:
    void* getInstance() override;
    void setInstance(void* instance) override;
    void write(DataOutputSerializer& out) override;
    void read(DataInputView& in) override;

private:
    void* instance_;
    TypeSerializer* serializer_;
};

#endif
