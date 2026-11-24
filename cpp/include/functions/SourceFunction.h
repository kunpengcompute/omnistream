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

#ifndef SOURCEFUNCTION_H
#define SOURCEFUNCTION_H

#include <type_traits>

#include "SourceContext.h"
#include "basictypes/callback.h"
#include "Function.h"

/**
 * T: such as Object
 * */
template <typename T>
class SourceFunction : public Object, public omnistream::Function {
public:
    virtual void run(SourceContext* context) = 0;
    virtual void cancel() = 0;

    // Intermediate operation, which can be deleted after dataStream shuffle is nativeized.
    void SaveCallBack(CallBack* callback)
    {
        this->callback = callback;
    }
    ~SourceFunction()
    {
        if (callback != nullptr) {
            delete callback;
        }
    }

protected:
    CallBack* callback = nullptr;
};

template <typename T>
using SourceFunctionUnique = std::unique_ptr<SourceFunction<T>>;

#endif // SOURCEFUNCTION_H
