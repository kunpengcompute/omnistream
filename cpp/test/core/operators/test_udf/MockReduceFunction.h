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

#ifndef FLINK_TNEL_MOCKREDUCEFUNCTION_H
#define FLINK_TNEL_MOCKREDUCEFUNCTION_H
#include "functions/ReduceFunction.h"
#include "basictypes/Long.h"
#include "basictypes/Tuple2.h"

class MockReduceFunction : public ReduceFunction<Object> {
public:
    Object* reduce(Object* in1, Object* in2) override
    {
        // 假设 in1 和 in2 都是 Tuple2，且 f1 是 Long
        auto tuple1 = dynamic_cast<Tuple2*>(in1);
        auto tuple2 = dynamic_cast<Tuple2*>(in2);
        if (tuple1 && tuple2) {
            auto long1 = dynamic_cast<Long*>(tuple1->f1);
            auto long2 = dynamic_cast<Long*>(tuple2->f1);
            if (long1 && long2) {
                long1->setValue(long1->getValue() + long2->getValue());
                return tuple1->clone();
            }
        }
        return nullptr;
    }
};
#endif // FLINK_TNEL_MOCKREDUCEFUNCTION_H
