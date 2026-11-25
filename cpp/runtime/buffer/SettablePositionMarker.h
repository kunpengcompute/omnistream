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

#pragma once
#include <iostream>
#include <cstdlib> // For abs function for integer types
#include <climits> // For INT_MIN

/***
class SettablePositionMarker
{
public:
    int get() const;
    bool isFinished() const;
    int getCached() const;

    int markFinished();

    void move(int offset);
    void set(int value);

    void commit();

private:
    volatile int position = 0;
    int cachedPosition = 0;
};

**/
