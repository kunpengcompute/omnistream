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

#include <stdexcept>
#include "NonReusingDeserializationDelegate.h"

NonReusingDeserializationDelegate::NonReusingDeserializationDelegate(
    omnistream::datastream::StreamElementSerializer* serializer)
    : serializer_(serializer),
      instance_(nullptr)
{
}

NonReusingDeserializationDelegate::~NonReusingDeserializationDelegate()
{
    if (serializer_ != nullptr) {
        delete serializer_;
        serializer_ = nullptr;
    }
}

void* NonReusingDeserializationDelegate::getInstance()
{
    return instance_;
}

void NonReusingDeserializationDelegate::setInstance(void* instance)
{
    instance_ = instance;
}

void NonReusingDeserializationDelegate::write(DataOutputSerializer& out)
{
    THROW_LOGIC_EXCEPTION("Serialization method called on DeserializationDelegate.");
}

void NonReusingDeserializationDelegate::read(DataInputView& in)
{
    LOG(">>>>>>>>>>");
    LOG("serializer_ is " << serializer_->getName());
    instance_ = serializer_->deserialize(in);
}
