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

#include "SettablePositionMarker.h"
/***
int SettablePositionMarker::get() const
{
    return position;
}

bool SettablePositionMarker::isFinished() const
{
    return position < 0;
}

int SettablePositionMarker::getCached() const
{
    return std::abs(position);
}

int SettablePositionMarker::markFinished()
{
    int currentPosition = getCached();
    int newValue = -currentPosition;
    if (newValue == 0)
    {
        newValue = INT_MIN;
    }
    set(newValue);
    return currentPosition;
}

void SettablePositionMarker::move(int offset)
{
    set(cachedPosition + offset);
}

void SettablePositionMarker::set(int value)
{
    cachedPosition = value;
}

void SettablePositionMarker::commit()
{
    position = cachedPosition;
}
**/
