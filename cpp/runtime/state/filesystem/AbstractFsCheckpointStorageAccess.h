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

#ifndef FLINK_TNEL_ABSTRACTFSCHECKPOINTSTORAGEACCESS_H
#define FLINK_TNEL_ABSTRACTFSCHECKPOINTSTORAGEACCESS_H
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/CheckpointStorageAccess.h"
#include "core/fs/Path.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "core/include/common.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/state/CheckpointStorageAccess.h"

namespace omnistream {

class AbstractFsCheckpointStorageAccess : public CheckpointStorageAccess {
public:
    inline static const std::string CHECKPOINT_DIR_PREFIX{"chk-"};
    inline static const std::string CHECKPOINT_SHARED_STATE_DIR{"shared"};
    inline static const std::string CHECKPOINT_TASK_OWNED_STATE_DIR{"taskowned"};
    inline static const std::string METADATA_FILE_NAME{"_metadata"};
    static constexpr unsigned char REFERENCE_MAGIC_NUMBER[4] = {0x05, 0x5F, 0x3F, 0x18};

protected:
    static std::shared_ptr<Path> createCheckpointDirectory(const Path& baseDirectory, int64_t checkpointId)
    {
        return std::make_shared<Path>(baseDirectory, std::string(CHECKPOINT_DIR_PREFIX) + std::to_string(checkpointId));
    };

    static std::shared_ptr<Path> decodePathFromReference(std::shared_ptr<CheckpointStorageLocationReference> reference)
    {
        if (reference->IsDefaultReference()) {
            THROW_LOGIC_EXCEPTION("Cannot decode default reference");
        }
        auto bytes = reference->GetReferenceBytes();
        size_t headerLen = 4;

        if (bytes->size() > headerLen) {
            for (size_t i = 0; i < headerLen; i++) {
                if (static_cast<unsigned char>(bytes->at(i)) != REFERENCE_MAGIC_NUMBER[i]) {
                    THROW_LOGIC_EXCEPTION("Reference starts with the wrong magic number");
                }
            }

            try {
                std::string pathStr(bytes->begin() + headerLen, bytes->end());
                return std::make_shared<Path>(pathStr);
            } catch (...) {
                THROW_LOGIC_EXCEPTION("Reference cannot be decoded to a path");
            }
        } else {
            THROW_LOGIC_EXCEPTION("Reference too short.");
        }
    };

    std::shared_ptr<Path> getCheckpointDirectoryForJob(const Path& baseDirectory, const omnistream::JobIDPOD& jobId)
    {
        return std::make_shared<Path>(baseDirectory, jobId.AbstractIDPOD::toString());
    }
};

} // namespace omnistream

#endif // FLINK_TNEL_ABSTRACTFSCHECKPOINTSTORAGEACCESS_H
