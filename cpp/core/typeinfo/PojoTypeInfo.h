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

#ifndef OMNISTREAM_POJOTYPEINFO_H
#define OMNISTREAM_POJOTYPEINFO_H
#include "TypeInformation.h"
#include "PojoField.h"
#include "PojoField.h"

class PojoTypeInfo : public TypeInformation {
public:
    PojoTypeInfo(std::string typeClass, std::vector<PojoField*> fields) : fields(fields), typeClass(typeClass) {};

    ~PojoTypeInfo() override
    {
        for (auto& field : fields) {
            delete field;
        }
    }

    TypeSerializer* createTypeSerializer() override;
    BackendDataType getBackendId() const override
    {
        return BackendDataType::POJO_BK;
    }
    std::string name() override
    {
        return "PojoTypeInfo";
    }

    TypeInformation* getTypeAt(int pos)
    {
        if (pos < 0 || pos >= static_cast<int>(fields.size())) {
            THROW_LOGIC_EXCEPTION("index out of bounds occurs in PojoTypeInfo.");
        }
        return fields[pos]->getTypeInformation();
    }

private:
    std::vector<PojoField*> fields;
    std::string typeClass;
};

#endif // OMNISTREAM_POJOTYPEINFO_H
