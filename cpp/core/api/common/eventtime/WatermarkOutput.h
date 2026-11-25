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

#ifndef OMNISTREAM_WATERMARKOUTPUT_H
#define OMNISTREAM_WATERMARKOUTPUT_H

#include "common.h"
#include "streaming/api/watermark/Watermark.h"

class WatermarkOutput {
public:
    virtual void emitWatermark(Watermark* watermark)
    {
    }

    virtual void MarkIdle()
    {
    }

    virtual void MarkActive()
    {
    }

    virtual ~WatermarkOutput() = default;
};
#endif // OMNISTREAM_WATERMARKOUTPUT_H
