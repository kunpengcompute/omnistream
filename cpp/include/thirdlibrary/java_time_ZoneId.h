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

#ifndef FLINK_TNEL_ZoneId_H
#define FLINK_TNEL_ZoneId_H

#include <string>
#include "basictypes/String.h"

class ZoneId : public Object {
public:
    ZoneId();

    ZoneId(std::string& tz);

    ~ZoneId();

    static ZoneId* systemDefault();

    std::string getTimezone();

private:
    std::string timezone;
};

#endif // FLINK_TNEL_ZoneId_H
