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

#ifndef FLINK_TNEL_OUTPUTTAG_H
#define FLINK_TNEL_OUTPUTTAG_H

#include <string>
#include "core/typeinfo/TypeInformation.h"

class OutputTag {
public:
    // OutputTag(std::string &id) : id(std::move(id)) {}

    OutputTag(std::string& id, TypeInformation* typeInfo) : id(std::move(id)), typeInfo(typeInfo)
    {
    }

    std::string getId() const
    {
        return id;
    }
    TypeInformation* getTypeInformation() const
    {
        return typeInfo;
    }
    bool isResponsibleFor(OutputTag* owner, OutputTag* other) const
    {
        return owner->equals(*other);
    }
    bool equals(OutputTag& other)
    {
        return other.id == this->id;
    }

private:
    std::string id;
    TypeInformation* typeInfo;
};
#endif // FLINK_TNEL_OUTPUTTAG_H
