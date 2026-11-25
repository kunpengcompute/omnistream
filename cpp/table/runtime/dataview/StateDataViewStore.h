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

#ifndef FLINK_TNEL_STATEDATAVIEWSTORE_H
#define FLINK_TNEL_STATEDATAVIEWSTORE_H
#include "StateMapView.h"
#include "core/typeutils/TypeSerializer.h"

class StateDataViewStore {
public:
    template <typename N, typename EK, typename EV>
    StateMapView<N, EK, EV> getStateMapView(
        const std::string& stateName,
        bool supportNullKey,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer) {};
};

#endif // FLINK_TNEL_STATEDATAVIEWSTORE_H
