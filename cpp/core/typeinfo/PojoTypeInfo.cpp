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

#include "PojoTypeInfo.h"
#include "core/typeutils/PojoSerializer.h"
TypeSerializer* PojoTypeInfo::createTypeSerializer()
{
    std::vector<TypeSerializer*> fieldSerializers;
    fieldSerializers.reserve(fields.size());
    std::vector<std::string> reflectiveFields;
    reflectiveFields.reserve(fields.size());
    std::sort(fields.begin(), fields.end(), [](const PojoField* a, const PojoField* b) {
        return a->getField().compare(b->getField()) < 0;
    });
    for (const auto& item : fields) {
        fieldSerializers.push_back(item->getTypeInformation()->createTypeSerializer());
        reflectiveFields.push_back(item->getField());
    }
    return new PojoSerializer(typeClass, fieldSerializers, reflectiveFields);
}
