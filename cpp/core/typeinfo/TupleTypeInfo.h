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

#ifndef OMNISTREAM_TUPLETYPEINFO_H
#define OMNISTREAM_TUPLETYPEINFO_H

#include "core/typeutils/TypeSerializer.h"
#include "TypeInformation.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

class TupleTypeInfo : public TypeInformation {
public:
    explicit TupleTypeInfo(TypeSerializer* typeSerializer) : typeSerializer(typeSerializer) {};

    explicit TupleTypeInfo(std::vector<TypeInformation*> types) : types(types)
    {
    }

    TypeSerializer* createTypeSerializer() override;
    TypeSerializer* getTypeSerializer() override;

    std::string name() override;

    ~TupleTypeInfo() override = default;

    static TupleTypeInfo* of(const json& type);

    BackendDataType getBackendId() const override;

private:
    TypeSerializer* typeSerializer = nullptr;
    std::vector<TypeInformation*> types;
};

#endif // OMNISTREAM_TUPLETYPEINFO_H
