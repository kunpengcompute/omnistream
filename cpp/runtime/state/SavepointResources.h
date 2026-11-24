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

#ifndef OMNISTREAM_SAVEPOINTRESOURCES_H
#define OMNISTREAM_SAVEPOINTRESOURCES_H
#include "state/FullSnapshotResources.h"
#include "state/SnapshotExecutionType.h"
class SavepointResources {
private:
    std::shared_ptr<FullSnapshotResources> snapshotResources_;
    SnapshotExecutionType preferredSnapshotExecutionType_;

public:
    SavepointResources(
        const std::shared_ptr<FullSnapshotResources>& snapshotResources,
        SnapshotExecutionType preferredSnapshotExecutionType)
        : snapshotResources_(snapshotResources),
          preferredSnapshotExecutionType_(preferredSnapshotExecutionType)
    {
    }
    const std::shared_ptr<FullSnapshotResources>& getSnapshotResources() const
    {
        return snapshotResources_;
    }
    SnapshotExecutionType getPreferredSnapshotExecutionType() const
    {
        return preferredSnapshotExecutionType_;
    }
};
#endif
