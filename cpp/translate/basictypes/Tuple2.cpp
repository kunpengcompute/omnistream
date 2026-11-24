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

#include "basictypes/Tuple2.h"

Tuple2::Tuple2(Object* f0, Object* f1)
{
    this->f0 = f0;
    f0->getRefCount();
    this->f1 = f1;
    f1->getRefCount();
}

Tuple2::Tuple2()
{
    f0 = nullptr;
    f1 = nullptr;
}

Tuple2::~Tuple2()
{
    if (f0 != nullptr) {
        f0->putRefCount();
    }
    if (f1 != nullptr) {
        f1->putRefCount();
    }
}
