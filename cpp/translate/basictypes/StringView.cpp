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

#include "basictypes/StringView.h"

StringView::StringView() = default;

StringView::StringView(char* pointer, size_t length)
{
    this->data = pointer;
    this->size = length;
}

StringView::~StringView() = default;

void StringView::setData(char* pointer)
{
    this->data = pointer;
}

void StringView::setSize(size_t length)
{
    this->size = length;
}

char* StringView::getData()
{
    return data;
}

size_t StringView::getSize()
{
    return size;
}

std::string_view StringView::getValue()
{
    return {data, size};
}

std::string StringView::toString()
{
    return std::string(data, size);
}
