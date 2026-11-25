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

#include "MutipleInputSelectionHandler.h"
#include "../../api/operators/InputSelection.h"

namespace omnistream {
int MutipleInputSelectionHandler::selectNextInputIndex(int lastReadInputIndex)
{
    uint64_t uavailableInputsMask = static_cast<uint64_t>(availableInputsMask);
    uint64_t unotFinishedInputsMask = static_cast<uint64_t>(notFinishedInputsMask);
    return InputSelection::fairSelectNextIndex(
        selectedInputsMask, uavailableInputsMask & unotFinishedInputsMask, lastReadInputIndex);
}
} // namespace omnistream
