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

#pragma once

namespace omnistream {

class InputChannel;

class InputWithData {
    /**
public:
    InputChannel *inputChannel;
    ReadOnlySlicedNetworkBuffer *networkBuffer;
    bool moreAvailable;

public:
    InputWithData(InputChannel *inputChannel, ReadOnlySlicedNetworkBuffer *networkBuffer, bool moreAvailable);

    InputChannel *getInputChannel();

    ReadOnlySlicedNetworkBuffer *getNetworkBuffer();

    bool isMoreAvailable();
    **/
};

} // namespace omnistream
