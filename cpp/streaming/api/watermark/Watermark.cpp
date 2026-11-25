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

#include "streaming/api/watermark/Watermark.h"

Watermark Watermark::MAX_WATERMARK(INT64_MAX);
Watermark Watermark::UNINITIALIZED(INT64_MIN);

Watermark::Watermark(int64_t timestamp) : timestamp_(timestamp)
{
    setTag(StreamElementTag::TAG_WATERMARK);
}

int64_t Watermark::getTimestamp() const
{
    return timestamp_;
}

void Watermark::setTimestamp(int64_t timestamp)
{
    timestamp_ = timestamp;
}
