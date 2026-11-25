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

#ifndef FLINK_TNEL_STRINGTYPEINFO_H
#define FLINK_TNEL_STRINGTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"

class StringTypeInfo : public TypeInformation {
public:
    explicit StringTypeInfo(const char* name);

    TypeSerializer* createTypeSerializer() override;
    std::string name() override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VARCHAR_BK;
    };

private:
    const char* name_ = TYPE_NAME_STRING;

    // u32string is std string for utf32 which is equivalent to java char utf32.
    const char* native_type = "std::u32string";
};

#endif
