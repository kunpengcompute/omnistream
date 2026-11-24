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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/restore/RocksDBHeapRestorePQState.h"

namespace {

class RecordingHeapPQWrapper : public HeapPriorityQueueSnapshotRestoreWrapperBase {
public:
    struct RestoredEntry {
        std::vector<int8_t> key;
        int keyGroupPrefixBytes;

        bool operator==(const RestoredEntry& other) const
        {
            return key == other.key && keyGroupPrefixBytes == other.keyGroupPrefixBytes;
        }
    };

    std::shared_ptr<StateMetaInfoSnapshot> snapshotMetaInfo() override
    {
        return nullptr;
    }

    std::unique_ptr<SingleStateIterator> createSnapshotIterator(int, int) override
    {
        return nullptr;
    }

    void restoreSerializedElement(const std::vector<int8_t>& serializedKey, int keyGroupPrefixBytes) override
    {
        restoredEntries.push_back({serializedKey, keyGroupPrefixBytes});
    }

    std::vector<RestoredEntry> restoredEntries;
};

} // namespace

TEST(RocksDBHeapRestorePQStateTest, RoutesSerializedKeyAndPrefixToHeapWrapper)
{
    auto wrapper = std::make_shared<RecordingHeapPQWrapper>();
    omnistream::RocksDBHeapRestorePQState writer(wrapper, 2);
    const std::vector<int8_t> key{1, 2, 0, 3};

    // Heap PQ restore consumes the serialized element from the key; RocksDB's value is intentionally ignored.
    writer.writeEntry(key, {9, 8, 7});

    ASSERT_EQ(wrapper->restoredEntries.size(), 1U);
    EXPECT_EQ(wrapper->restoredEntries[0], (RecordingHeapPQWrapper::RestoredEntry{key, 2}));
}

TEST(RocksDBHeapRestorePQStateTest, PreservesEmptyKeyAndConfiguredPrefix)
{
    auto wrapper = std::make_shared<RecordingHeapPQWrapper>();
    omnistream::RocksDBHeapRestorePQState writer(wrapper, 0);

    writer.writeEntry({}, {1});

    ASSERT_EQ(wrapper->restoredEntries.size(), 1U);
    EXPECT_EQ(wrapper->restoredEntries[0], (RecordingHeapPQWrapper::RestoredEntry{{}, 0}));
}

TEST(RocksDBHeapRestorePQStateTest, FlushAndDiscardDoNotAddRestoreEntries)
{
    auto wrapper = std::make_shared<RecordingHeapPQWrapper>();
    omnistream::RocksDBHeapRestorePQState writer(wrapper, 1);
    writer.writeEntry({4, 5}, {});

    writer.flush();
    writer.discard();

    EXPECT_EQ(wrapper->restoredEntries, (std::vector<RecordingHeapPQWrapper::RestoredEntry>{{{4, 5}, 1}}));
}
