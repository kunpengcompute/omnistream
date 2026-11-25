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

#ifndef OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H
#define OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H

#include "runtime/state/StreamStateHandle.h"

namespace omnistream {

class CompletedCheckpointStorageLocation {
public:
    virtual ~CompletedCheckpointStorageLocation() = default;

    virtual StreamStateHandle* getMetadataHandle() = 0;
    virtual std::string getExternalPointer() = 0;
    virtual void disposeStorageLocation() = 0;
};
} // namespace omnistream

#endif // OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H
