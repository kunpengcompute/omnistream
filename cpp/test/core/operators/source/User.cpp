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

#include "User.h"
#include "basictypes/ReflectMacros.h"

DEFINE_REFLECT_CLASS_BEGIN(User)
REGISTER_PTR_FIELD(User, name, String*, "java_lang_String")
REGISTER_PRIMITIVE_FIELD(User, age, Long, "java_lang_Long")
DEFINE_REFLECT_CLASS_END(User)
