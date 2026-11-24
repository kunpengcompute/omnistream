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

#include "TableFunctionCollector.h"

void TableFunctionCollector::reset()
{
    collected = false;
    /* todo: This if is weird, it will have infinitely deep recursive call.
    if (auto *castedCollector = static_cast<TableFunctionCollector *>(collector)) {
        castedCollector->reset();
    }
     */
}
void TableFunctionCollector::outputResult(void* result)
{
    collected = true;
    this->collector->collect(result);
}
