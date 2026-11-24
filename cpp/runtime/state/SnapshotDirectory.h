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

#ifndef OMNISTREAM_SNAPSHOTDIRECTORY_H
#define OMNISTREAM_SNAPSHOTDIRECTORY_H

#include <filesystem>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <vector>
#include "DirectoryStateHandle.h"
#include "core/utils/FileUtils.h"

namespace fs = std::filesystem;
class TemporarySnapshotDirectory;
class PermanentSnapshotDirectory;

class SnapshotDirectory {
public:
    enum class State {
        ONGOING,
        COMPLETED,
        DELETED
    };

    SnapshotDirectory(const SnapshotDirectory&) = delete;
    SnapshotDirectory& operator=(const SnapshotDirectory&) = delete;

    const fs::path& getDirectory() const
    {
        return directory_;
    }

    void mkdirs() const
    {
        fs::create_directories(directory_);
    }

    bool exists() const
    {
        return fs::exists(directory_);
    }

    std::vector<fs::path> listDirectory() const
    {
        return FileUtils::listDirectory(directory_);
    }

    void cleanup()
    {
        if (state_.compare_exchange_strong(ongoingState_, State::DELETED)) {
            FileUtils::deleteDirectory(directory_);
        }
    }

    bool isSnapshotCompleted() const
    {
        return state_.load() == State::COMPLETED;
    }

    virtual std::unique_ptr<DirectoryStateHandle> completeSnapshotAndGetHandle() = 0;

    virtual ~SnapshotDirectory() = default;

protected:
    explicit SnapshotDirectory(const fs::path& directory)
        : directory_(directory),
          state_(State::ONGOING),
          ongoingState_(State::ONGOING)
    {
    }

    fs::path directory_;
    std::atomic<State> state_;
    State ongoingState_ = State::ONGOING;
};

class TemporarySnapshotDirectory : public SnapshotDirectory {
public:
    explicit TemporarySnapshotDirectory(const fs::path& directory) : SnapshotDirectory(directory)
    {
    }

    std::unique_ptr<DirectoryStateHandle> completeSnapshotAndGetHandle() override
    {
        return nullptr;
    }
};

class PermanentSnapshotDirectory : public SnapshotDirectory {
public:
    explicit PermanentSnapshotDirectory(const fs::path& directory) : SnapshotDirectory(directory)
    {
    }

    std::unique_ptr<DirectoryStateHandle> completeSnapshotAndGetHandle() override
    {
        State current = state_.load();
        if (current == State::COMPLETED) {
            return DirectoryStateHandle::forPathWithSizePtr(Path(directory_.string()));
        }

        State expected = State::ONGOING;
        if (state_.compare_exchange_strong(expected, State::COMPLETED)) {
            return DirectoryStateHandle::forPathWithSizePtr(Path(directory_.string()));
        } else {
            if (expected == State::COMPLETED) {
                return DirectoryStateHandle::forPathWithSizePtr(Path(directory_.string()));
            } else {
                throw std::runtime_error("Snapshot state is invalid");
            }
        }
    }
};

#endif // OMNISTREAM_SNAPSHOTDIRECTORY_H
