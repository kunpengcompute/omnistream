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

#ifndef OMNISTREAM_KEYSELECTOR_H
#define OMNISTREAM_KEYSELECTOR_H

#include "functions/KeySelect.h"
#include "functions/MapFunction.h"
#include "basictypes/Object.h"
#include "basictypes/Tuple2.h"

class MockKeyedBy : public KeySelect<Object> {
public:
    Object* getKey(Object* value) override
    {
        auto* tuple = reinterpret_cast<Tuple2*>(value);
        return tuple->f0;
    }
};

#endif // OMNISTREAM_KEYSELECTOR_H
