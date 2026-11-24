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
#include <experimental/filesystem>
#include <sstream>
#include <random>
#include <chrono>
#include <fstream>
#include <filesystem>
#include "runtime/state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/StreamStateHandleFactory.h"

class DummyStreamStateHandle : public StreamStateHandle {
public:
    DummyStreamStateHandle(long size = 123, std::string v = "dummy") : size_(size), value_(v)
    {
    }
    void DiscardState() override
    {
    }
    long GetStateSize() const override
    {
        return size_;
    }
    std::string ToString() const override
    {
        return value_;
    }
    bool operator==(const StreamStateHandle& o) const
    {
        auto* other = dynamic_cast<const DummyStreamStateHandle*>(&o);
        return other && value_ == other->value_ && size_ == other->size_;
    }
    std::size_t hashCode() const
    {
        return std::hash<std::string>()(value_);
    }

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        return nullptr;
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        return std::nullopt;
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return PhysicalStateHandleID("");
    }

private:
    long size_;
    std::string value_;
};

class DummyDirectoryStateHandle : public DirectoryStateHandle {
public:
    DummyDirectoryStateHandle(std::string s = "dir", long sz = 888) : DirectoryStateHandle(s, sz)
    {
    }
    void DiscardState() override
    {
    }
    std::string ToString() const override
    {
        return "DirDummy";
    }
};

TEST(IncrementalLocalKeyedStateHandleTest, BasicConstructAndGetter)
{
    UUID uuid(1, 2);
    int64_t checkpointId = 77;
    DummyDirectoryStateHandle dirHandle("d", 555);
    KeyGroupRange keyGroupRange(0, 2);
    auto metaState = std::make_shared<DummyStreamStateHandle>(111, "meta");
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;

    IncrementalLocalKeyedStateHandle handle(uuid, checkpointId, &dirHandle, keyGroupRange, metaState, sharedState);

    EXPECT_EQ(handle.GetCheckpointId(), checkpointId);
    EXPECT_EQ(handle.GetBackendIdentifier(), uuid);
    EXPECT_EQ(handle.GetMetaDataState(), metaState);
    EXPECT_EQ(handle.getDirectoryStateHandle(), &dirHandle);
    EXPECT_EQ(handle.GetSharedStateHandles().size(), sharedState.size());
}

TEST(IncrementalLocalKeyedStateHandleTest, ReboundCreatesNewHandle)
{
    UUID uuid(123, 456);
    int64_t checkpointId = 100;
    auto dirHandle = std::make_shared<DummyDirectoryStateHandle>("data", 888);
    KeyGroupRange keyGroupRange(3, 8);
    auto metaState = std::make_shared<DummyStreamStateHandle>(33, "meta33");
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;

    IncrementalLocalKeyedStateHandle handle(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);

    int64_t newCheckpointId = 555;
    auto rebound = handle.rebound(newCheckpointId);

    ASSERT_NE(rebound, nullptr);
    auto* newHandle = dynamic_cast<IncrementalLocalKeyedStateHandle*>(rebound.get());
    ASSERT_TRUE(newHandle != nullptr);
    EXPECT_EQ(newHandle->GetCheckpointId(), newCheckpointId);
    EXPECT_EQ(newHandle->GetBackendIdentifier(), uuid);
    EXPECT_EQ(newHandle->GetMetaDataState(), metaState);
    EXPECT_EQ(newHandle->getDirectoryStateHandle(), dirHandle.get());
    EXPECT_EQ(newHandle->GetSharedStateHandles().size(), sharedState.size());
}

TEST(IncrementalLocalKeyedStateHandleTest, EqualsAndHashCode)
{
    UUID uuid(42, 99);
    int64_t checkpointId = 1;
    auto dirHandle = std::make_shared<DummyDirectoryStateHandle>("d1", 10);
    KeyGroupRange keyGroupRange(1, 1);
    auto metaState = std::make_shared<DummyStreamStateHandle>(8, "s");
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;

    IncrementalLocalKeyedStateHandle h1(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);
    IncrementalLocalKeyedStateHandle h2(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);

    EXPECT_TRUE(h1 == h2);
    EXPECT_FALSE(h1 != h2);
    EXPECT_EQ(h1.hashCode(), h2.hashCode());

    auto meta2 = std::make_shared<DummyStreamStateHandle>(88, "diff");
    IncrementalLocalKeyedStateHandle h3(uuid, checkpointId, dirHandle.get(), keyGroupRange, meta2, sharedState);
    EXPECT_FALSE(h1 == h3);
}

TEST(IncrementalLocalKeyedStateHandleTest, GetStateSizeAccumulates)
{
    UUID uuid(2, 3);
    int64_t checkpointId = 101;
    auto dirHandle = std::make_shared<DummyDirectoryStateHandle>("xx", 777);
    KeyGroupRange keyGroupRange(0, 1);
    auto metaState = std::make_shared<DummyStreamStateHandle>(33, "a");
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;

    IncrementalLocalKeyedStateHandle handle(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);

    EXPECT_EQ(handle.GetStateSize(), 777 + 33); // directory + meta
}

TEST(IncrementalLocalKeyedStateHandleTest, DiscardStateDoesNotThrow)
{
    UUID uuid(6, 7);
    int64_t checkpointId = 200;
    auto dirHandle = std::make_shared<DummyDirectoryStateHandle>("x", 1);
    KeyGroupRange keyGroupRange(0, 0);
    auto metaState = std::make_shared<DummyStreamStateHandle>(0, "meta");
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;

    IncrementalLocalKeyedStateHandle handle(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);

    EXPECT_NO_THROW(handle.DiscardState());
}

TEST(IncrementalLocalKeyedStateHandleTest, SharedStateHandlesWorks)
{
    UUID uuid(13, 14);
    int64_t checkpointId = 666;
    auto dirHandle = std::make_shared<DummyDirectoryStateHandle>("xx", 77);
    KeyGroupRange keyGroupRange(2, 5);
    auto metaState = std::make_shared<DummyStreamStateHandle>(99, "m");

    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedState;
    sharedState.push_back(
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(std::make_shared<DummyStreamStateHandle>(1, "a"), "id1"));
    sharedState.push_back(
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(std::make_shared<DummyStreamStateHandle>(2, "b"), "id2"));

    IncrementalLocalKeyedStateHandle handle(uuid, checkpointId, dirHandle.get(), keyGroupRange, metaState, sharedState);

    EXPECT_EQ(handle.GetSharedStateHandles().size(), 2);

    bool hasId1 = false, hasId2 = false;
    for (const auto& h : handle.GetSharedStateHandles()) {
        if (h.getLocalPath() == "id1") hasId1 = true;
        if (h.getLocalPath() == "id2") hasId2 = true;
    }
    EXPECT_TRUE(hasId1);
    EXPECT_TRUE(hasId2);
}

TEST(IncrementalLocalKeyedStateHandleTest, JSONConstructorAndToString)
{
    std::string dirName = (std::filesystem::temp_directory_path() / "IncLocalKeyedHandleTest").string();
    std::string fileName = "metaState.bin";
    std::string content = "TestFileForIncLocalKeyedHandle";
    std::filesystem::remove_all(dirName);
    std::filesystem::create_directories(dirName);
    std::ofstream ofs(dirName + "/" + fileName, std::ios::binary);
    ofs << content;
    ofs.close();

    Path path(dirName);
    DirectoryStateHandle originalDirHandle = DirectoryStateHandle::forPathWithSize(path);
    long expectedDirSize = originalDirHandle.GetStateSize();

    std::string metaFilePath = dirName + "/" + fileName;
    Path metaPath(metaFilePath);
    long metaStateSize = std::filesystem::file_size(metaFilePath);

    nlohmann::json metaDataState = {
        {"@class", "org.apache.flink.runtime.state.filesystem.FileStateHandle"},
        {"stateHandleName", "FileStateHandle"},
        {"streamStateHandleID",
         {{"@class", "org.apache.flink.runtime.state.PhysicalStateHandleID"}, {"keyString", metaPath.toString()}}},
        {"stateSize", metaStateSize}};

    nlohmann::json jsonObj;

    jsonObj["checkpointId"] = 1234;
    jsonObj["backendIdentifier"] = "01234567-89ab-cdef-0123-456789abcdef";
    jsonObj["metaDataState"] = metaDataState;
    jsonObj["sharedState"] = nlohmann::json::array();
    EXPECT_THROW({ DirectoryKeyedStateHandle handle(jsonObj); }, std::invalid_argument);

    jsonObj["directoryStateHandle"] = {
        {"@class", "org.apache.flink.runtime.state.DirectoryStateHandle"},
        {"directoryString", path.toString()},
        {"stateSize", expectedDirSize},
        {"directory", "file://" + path.toString() + "/"},
    };
    EXPECT_THROW({ DirectoryKeyedStateHandle handle(jsonObj); }, std::invalid_argument);

    jsonObj["keyGroupRange"] = {{"startKeyGroup", 0}, {"endKeyGroup", 9}};

    nlohmann::json sharedHandle1 = {
        {"handle",
         {{"@class", "org.apache.flink.runtime.state.filesystem.FileStateHandle"},
          {"stateHandleName", "FileStateHandle"},
          {"filePath", metaPath.toString()},
          {"stateSize", metaStateSize}}},
        {"localPath", "/local/path/1"}};

    nlohmann::json sharedHandle2 = {{"handle", nullptr}, {"localPath", "/local/path/2"}};

    jsonObj["sharedState"] = nlohmann::json::array({sharedHandle1, sharedHandle2});

    IncrementalLocalKeyedStateHandle handle(jsonObj);

    EXPECT_EQ(handle.GetCheckpointId(), 1234);
    EXPECT_EQ(handle.GetBackendIdentifier(), UUID::FromString("01234567-89ab-cdef-0123-456789abcdef"));
    EXPECT_EQ(handle.GetKeyGroupRange(), KeyGroupRange(0, 9));
    ASSERT_TRUE(handle.GetMetaDataState() != nullptr);

    auto metaStatePtr = std::dynamic_pointer_cast<FileStateHandle>(handle.GetMetaDataState());
    ASSERT_TRUE(metaStatePtr != nullptr);
    EXPECT_EQ(metaStatePtr->GetStateSize(), metaStateSize);

    auto dirHandle = handle.getDirectoryStateHandle();
    ASSERT_TRUE(dirHandle != nullptr);
    EXPECT_EQ(dirHandle->GetStateSize(), expectedDirSize);

    std::string toStringResult = handle.ToString();
    std::cout << "toStringResult = " << toStringResult << std::endl;

    auto parsed = nlohmann::json::parse(toStringResult);
    std::cout << parsed.dump() << std::endl;
    EXPECT_EQ(parsed["stateHandleName"], "IncrementalLocalKeyedStateHandle");
    EXPECT_EQ(parsed["checkpointId"], 1234);
    EXPECT_EQ(parsed["backendIdentifier"], "01234567-89ab-cdef-0123-456789abcdef");
    EXPECT_EQ(parsed["metaDataState"]["@class"], "org.apache.flink.runtime.state.filesystem.FileStateHandle");
    EXPECT_EQ(parsed["metaDataState"]["filePath"], metaPath.toString());
    EXPECT_EQ(parsed["metaDataState"]["stateSize"], metaStateSize);
    EXPECT_EQ(parsed["directoryStateHandle"]["directory"], path.toString());
    EXPECT_EQ(parsed["keyGroupRange"]["startKeyGroup"], 0);
    EXPECT_EQ(parsed["keyGroupRange"]["endKeyGroup"], 9);

    ASSERT_EQ(parsed["sharedState"].size(), 2);
    EXPECT_EQ(
        parsed["sharedState"][0]["handle"]["@class"], "org.apache.flink.runtime.state.filesystem.FileStateHandle");
    EXPECT_EQ(parsed["sharedState"][0]["localPath"], "/local/path/1");
    EXPECT_TRUE(parsed["sharedState"][1]["handle"].is_null());
    EXPECT_EQ(parsed["sharedState"][1]["localPath"], "/local/path/2");

    std::filesystem::remove_all(dirName);
}
