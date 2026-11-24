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

#include "basictypes/java_util_Map_Entry.h"

MapEntry::MapEntry(Object* key, Object* value)
{
    key_ = key;
    value_ = value;
    key->getRefCount();
    value->getRefCount();
}

MapEntry::MapEntry()
{
    key_ = nullptr;
    value_ = nullptr;
}

MapEntry::~MapEntry()
{
    if (key_ != nullptr) {
        key_->putRefCount();
    }
    if (value_ != nullptr) {
        value_->putRefCount();
    }
}

Object* MapEntry::getKey() const
{
    return key_;
}

const Object* MapEntry::getValue() const
{
    return value_;
}

Object* MapEntry::getKey()
{
    return key_;
}

Object* MapEntry::getValue()
{
    return value_;
}

void MapEntry::setValue(Object* newValue)
{
    newValue->getRefCount();
    value_->putRefCount();
    value_ = newValue;
}
