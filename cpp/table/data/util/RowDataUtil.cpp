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

#include "RowDataUtil.h"

bool RowDataUtil::isAccumulateMsg(RowKind kind)
{
    return kind == RowKind::INSERT || kind == RowKind::UPDATE_AFTER;
}

bool RowDataUtil::isRetractMsg(RowKind kind)
{
    return kind == RowKind::UPDATE_BEFORE || kind == RowKind::DELETE;
}
