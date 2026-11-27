/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "AvailabilityProvider.h"
#include <sstream>

namespace omnistream {

std::shared_ptr<CompletableFuture> AvailabilityProvider::AVAILABLE = std::make_shared<CompletableFuture>(true);

std::shared_ptr<CompletableFuture> AvailabilityProvider::and_(
    std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second)
{
    if (first == AVAILABLE && second == AVAILABLE) {
        return AVAILABLE;
    } else if (first == AVAILABLE) {
        return second;
    } else if (second == AVAILABLE) {
        return first;
    } else {
        return CompletableFuture::allOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
    }
}

std::shared_ptr<CompletableFuture> AvailabilityProvider::or_(
    std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second)
{
    if (first == AVAILABLE || second == AVAILABLE) {
        return AVAILABLE;
    }
    return CompletableFuture::anyOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
}

} // namespace omnistream
