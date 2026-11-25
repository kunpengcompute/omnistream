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

#include "TimerTypeInfo.h"
#include "table/runtime/operators/TimerSerializer.h"

TimerTypeInfo::TimerTypeInfo(
    TypeInformation* keyTypeInfo_, TypeInformation* namespaceTypeInfo_, Class* keyClazz_, Class* namespaceClazz_)
    : keyTypeInfo(keyTypeInfo_),
      namespaceTypeInfo(namespaceTypeInfo_),
      keyClazz(keyClazz_),
      namespaceClazz(namespaceClazz_)
{
    keyTypeInfo_->getRefCount();
    namespaceTypeInfo->getRefCount();

    keyClazz_->getRefCount();
    namespaceClazz_->getRefCount();
}

TimerTypeInfo::~TimerTypeInfo()
{
    keyTypeInfo->putRefCount();
    namespaceTypeInfo->putRefCount();

    keyClazz->putRefCount();
    namespaceClazz->putRefCount();
}

TypeSerializer* TimerTypeInfo::createTypeSerializer()
{
    return new TimerSerializer<Object*, Object*>(
        keyTypeInfo->createTypeSerializer(), namespaceTypeInfo->createTypeSerializer(), keyClazz, namespaceClazz);
}
