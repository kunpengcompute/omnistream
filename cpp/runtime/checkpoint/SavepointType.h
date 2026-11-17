/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef OMNISTREAM_SAVEPOINTTYPE
#define OMNISTREAM_SAVEPOINTTYPE

#include "SnapshotType.h"

enum class SavepointFormatType {
    CANONICAL,
    NATIVE,
    COMPATIBLE // FLINK格式
};

class SavepointType : public SnapshotType {
public:
    enum class PostCheckpointAction {
        NONE,
        SUSPEND,
        TERMINATE
    };

    static std::shared_ptr<SavepointType> savepoint(SavepointFormatType formatType)
    {
        return std::shared_ptr<SavepointType>(new SavepointType("Savepoint", PostCheckpointAction::NONE, formatType));
    }

    static std::shared_ptr<SavepointType> terminate(SavepointFormatType formatType)
    {
        return std::shared_ptr<SavepointType>(
            new SavepointType("Terminate Savepoint", PostCheckpointAction::TERMINATE, formatType));
    }

    static std::shared_ptr<SavepointType> suspend(SavepointFormatType formatType)
    {
        return std::shared_ptr<SavepointType>(
            new SavepointType("Suspend Savepoint", PostCheckpointAction::SUSPEND, formatType));
    }

    bool IsSavepoint() const override
    {
        return true;
    }

    bool isSynchronous()
    {
        return postCheckpointAction_ != PostCheckpointAction::NONE;
    }

    PostCheckpointAction getPostCheckpointAction()
    {
        return postCheckpointAction_;
    }

    bool shouldAdvanceToEndOfTime()
    {
        return shouldDrain();
    }

    bool shouldDrain()
    {
        return getPostCheckpointAction() == PostCheckpointAction::TERMINATE;
    }

    bool shouldIgnoreEndOfInput()
    {
        return getPostCheckpointAction() == PostCheckpointAction::SUSPEND;
    }

    std::string GetName() const override
    {
        return name_;
    }

    SavepointFormatType getFormatType()
    {
        return formatType_;
    }

    SharingFilesStrategy GetSharingFilesStrategy() const override
    {
        return SharingFilesStrategy::NO_SHARING;
    }

    bool operator==(const SnapshotType& other) const override
    {
        if (!other.IsSavepoint()) {
            return false;
        }
        auto otherCasted = dynamic_cast<const SavepointType&>(other);
        return this->name_ == otherCasted.name_ && this->postCheckpointAction_ == otherCasted.postCheckpointAction_ &&
               this->formatType_ == otherCasted.formatType_;
    }

    std::string ToString() override
    {
        nlohmann::json json;
        json["name"] = name_;
        json["postCheckpointAction"] = postCheckpointAction_;
        json["formatType"] = formatType_;
        return json.dump();
    }

    nlohmann::json ToJson() override
    {
        nlohmann::json json;
        json["name"] = name_;
        json["postCheckpointAction"] = postCheckpointAction_;
        json["formatType"] = formatType_;
        return json;
    }

private:
    SavepointType(std::string name, PostCheckpointAction postCheckpointAction, SavepointFormatType formatType)
        : name_(name),
          postCheckpointAction_(postCheckpointAction),
          formatType_(formatType) {};

    std::string name_;
    PostCheckpointAction postCheckpointAction_;
    SavepointFormatType formatType_;
};

#endif // OMNISTREAM_SAVEPOINTTYPE
