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

#ifndef FLINK_TNEL_STRINGVIEW_H
#define FLINK_TNEL_STRINGVIEW_H

#include "String.h"

/**
 * data come from other, which is used in kafka source operator
 */
class StringView : public String {
public:
    StringView();

    StringView(char* pointer, size_t length);

    ~StringView();

    void setData(char* pointer) override;

    void setSize(size_t length) override;

    char* getData() override;

    size_t getSize() override;

    std::string_view getValue() override;

    std::string toString() override;

private:
    char* data;
    size_t size;
};

#endif // FLINK_TNEL_STRINGVIEW_H
