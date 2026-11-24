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

#ifndef OMNISTREAM_BACKENDRESTORERPROCEDURE
#define OMNISTREAM_BACKENDRESTORERPROCEDURE

#include <string>
#include <vector>
#include <set>
#include <functional>
#include "runtime/state/KeyedStateHandle.h"

template <typename T, typename S>
class BackendRestorerProcedure {
public:
    BackendRestorerProcedure(std::function<T(std::set<S>, int)> instanceSupplier, std::string logDescription)
        : instanceSupplier_(instanceSupplier),
          logDescription_(logDescription) {};

    T createAndRestore(std::vector<std::set<S>> restoreOptions)
    {
        if (restoreOptions.empty()) {
            restoreOptions.push_back(std::set<S>());
        }

        size_t alternativeIdx = 0;
        while (alternativeIdx < restoreOptions.size()) {
            std::set<S> restoreState = restoreOptions[alternativeIdx];
            auto curIdx = alternativeIdx;

            ++alternativeIdx;

            try {
                return attemptCreateAndRestore(restoreState, curIdx);
            } catch (const std::exception& e) {
                throw;
            }
        }
        throw std::runtime_error("createAndRestore exception.");
    }

private:
    T attemptCreateAndRestore(std::set<S> restoreState, int alternativeIdx)
    {
        T backendInstance = instanceSupplier_(restoreState, alternativeIdx);

        try {
            // register closable into backendCloseableRegistry
            return backendInstance;
        } catch (const std::exception& e) {
            try {
                backendInstance->dispose();
            } catch (const std::exception& ex) {
                throw std::runtime_error("dispose failed.");
            }
            throw;
        }
    }

    std::function<T(std::set<S>, int)> instanceSupplier_;
    std::string logDescription_;
};

#endif // OMNISTREAM_BACKENDRESTORERPROCEDURE
