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

#include "ObjectBufferRecycler.h"

#include <memory>
namespace omnistream {
// Define the static member
std::shared_ptr<DummyObjectBufferRecycler> DummyObjectBufferRecycler::instance = nullptr;

std::shared_ptr<DeepCopiedObjectBufferRecycler> DeepCopiedObjectBufferRecycler::instance = nullptr;
} // namespace omnistream
