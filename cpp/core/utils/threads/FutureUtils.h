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

#ifndef OMNISTREAM_FUTUREUTILS_H
#define OMNISTREAM_FUTUREUTILS_H
#include <future>

class FutureUtils {
public:
    template <typename T>
    static T runIfNotDoneAndGet(std::shared_ptr<std::packaged_task<T()>> task)
    {
        if (!task) {
            return T();
        }

        std::future<T> fut = task->get_future();

        auto status = fut.wait_for(std::chrono::seconds(0));
        if (status != std::future_status::ready) {
            (*task)();
        }

        try {
            return fut.get();
        } catch (const std::exception& e) {
            throw;
        }
    }
};

#endif // OMNISTREAM_FUTUREUTILS_H
