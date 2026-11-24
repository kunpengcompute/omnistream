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

#ifndef FLINK_TNEL_VALUECOLLECTOR_H
#define FLINK_TNEL_VALUECOLLECTOR_H

#include "../../../core/include/common.h"

// this is specific for UDF to output the result value
class ValueCollector {
public:
    // value can be int, string, date POJO.
    // ownership, one the object is passed into collector from UDF, the ownership  of object are transferred as well
    // it is collector to decide whether delete it, keep it or transfer it again.
    virtual void collect(void* value) = 0;
    virtual void collectRow(void* value) = 0;
};

#endif
