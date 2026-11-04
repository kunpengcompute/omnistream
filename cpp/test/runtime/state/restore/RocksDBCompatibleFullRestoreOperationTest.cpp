/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RocksDBCompatibleFullRestoreOperation.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

StateMetaInfoSnapshot makeMetaInfo()
{
    return StateMetaInfoSnapshot(
        "state",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

std::shared_ptr<KeyedStateHandle> makeKeyedStateHandle()
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange);
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("rocks-compatible-restore", std::vector<uint8_t>{1});
    return std::make_shared<KeyGroupsStateHandle>(offsets, streamHandle);
}

FlinkSavepointAdaptorInfo makeAdaptorInfo()
{
    return {FlinkSavepointAdaptorType::GroupAggAdaptor, "group agg compatible adaptor"};
}

template <typename ExpectedException, typename Action>
void expectThrowsWithMessage(Action&& action, const std::string& expectedMessage)
{
    try {
        std::forward<Action>(action)();
        FAIL() << "Expected exception was not thrown";
    } catch (const ExpectedException& exception) {
        EXPECT_EQ(typeid(exception), typeid(ExpectedException));
        EXPECT_EQ(exception.what(), expectedMessage);
    } catch (const std::exception& exception) {
        FAIL() << "Unexpected std::exception type with message: " << exception.what();
    } catch (...) {
        FAIL() << "Unexpected non-standard exception";
    }
}

class RecordingRestoreAdaptor : public omnistream::OperatorSavepointAdaptor {
public:
    explicit RecordingRestoreAdaptor(std::vector<std::string>* events) : events_(events)
    {
    }

    void prepareForRestore(const nlohmann::json& operatorDescription) override
    {
        events_->push_back("prepare");
        operatorName_ = operatorDescription.value("operator", "");
    }

    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override
    {
        events_->push_back("validate");
        validatedMetaCounts_.push_back(metaInfos.size());
        if (failOnEmptyMetadata_ && metaInfos.empty()) {
            throw std::runtime_error("empty metadata rejected");
        }
        if (throwOnValidate_) {
            throw std::runtime_error("validate failed");
        }
    }

    void save(CheckpointStateOutputStreamProxy&, KeyGroupRangeOffsets&, FullSnapshotResources&, std::string) override
    {
    }

    void restore(SavepointRestoreResultIterator& restoreIterator, omnistream::RestoreBackendDelegate&) override
    {
        events_->push_back("restore");
        actualIteratorHadNext_ = restoreIterator.hasNext();
        if (onRestore_) {
            onRestore_();
        }
        if (throwOnRestore_) {
            throw std::runtime_error("restore failed");
        }
    }

    std::vector<std::string>* events_;
    bool failOnEmptyMetadata_ = false;
    bool throwOnValidate_ = false;
    bool throwOnRestore_ = false;
    bool actualIteratorHadNext_ = false;
    std::vector<size_t> validatedMetaCounts_;
    std::string operatorName_;
    std::function<void()> onRestore_;
};

class RocksDBCompatibleFullRestoreOperationTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        ON_CALL(*bridge_, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{makeMetaInfo()}));
        ON_CALL(*bridge_, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));
        ON_CALL(*bridge_, closeSavepointInputStream(_)).WillByDefault(Return());

        const auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ = std::filesystem::temp_directory_path() /
                  ("rocks-compatible-restore-" + std::string(testInfo->test_suite_name()) + "-" +
                   std::string(testInfo->name()) + "-" + std::to_string(nextDbPathId_++));
        std::filesystem::remove_all(dbPath_);
        dbOptions_ = std::make_shared<rocksdb::DBOptions>();
        dbOptions_->create_if_missing = true;
    }

    void TearDown() override
    {
        std::filesystem::remove_all(dbPath_);
    }

    std::unique_ptr<RocksDBCompatibleFullRestoreOperation<int>> makeOperation(
        std::vector<std::shared_ptr<KeyedStateHandle>> handles, std::unique_ptr<RecordingRestoreAdaptor> adaptor)
    {
        return makeOperation(std::move(handles), std::move(adaptor), bridge_);
    }

    std::unique_ptr<RocksDBCompatibleFullRestoreOperation<int>> makeOperation(
        std::vector<std::shared_ptr<KeyedStateHandle>> handles,
        std::unique_ptr<RecordingRestoreAdaptor> adaptor,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge)
    {
        return std::make_unique<RocksDBCompatibleFullRestoreOperation<int>>(
            &keyGroupRange_,
            2, // keyGroupPrefixBytes
            nullptr,
            &kvStateInformation_,
            dbPath_,
            dbOptions_,
            [](const std::string&) { return rocksdb::ColumnFamilyOptions(); },
            std::move(handles),
            2097152L, // writeBatchSize
            std::move(bridge),
            makeAdaptorInfo(),
            std::move(adaptor));
    }

    void expectPathCanBeOpenedAgain()
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
        descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());

        rocksdb::DBOptions reopenOptions;
        reopenOptions.create_if_missing = true;
        rocksdb::DB* reopenedDb = nullptr;
        std::vector<rocksdb::ColumnFamilyHandle*> reopenedHandles;
        auto status = rocksdb::DB::Open(reopenOptions, dbPath_.string(), descriptors, &reopenedHandles, &reopenedDb);

        ASSERT_TRUE(status.ok()) << "Failed to reopen RocksDB path: " << status.ToString();
        ASSERT_NE(reopenedDb, nullptr);
        ASSERT_EQ(reopenedHandles.size(), 1U);

        status = reopenedDb->DestroyColumnFamilyHandle(reopenedHandles[0]);
        EXPECT_TRUE(status.ok()) << "Failed to destroy reopened default column family: " << status.ToString();
        status = reopenedDb->Close();
        EXPECT_TRUE(status.ok()) << "Failed to close reopened RocksDB: " << status.ToString();
        delete reopenedDb;
    }

    void closeRestoreResult(const std::shared_ptr<RocksDBRestoreResult>& result)
    {
        ASSERT_NE(result, nullptr);
        auto* db = result->getDb();
        ASSERT_NE(db, nullptr);

        std::vector<rocksdb::ColumnFamilyHandle*> stateHandles;
        for (const auto& [stateName, stateInfo] : kvStateInformation_) {
            static_cast<void>(stateName);
            if (stateInfo != nullptr && stateInfo->columnFamilyHandle_ != nullptr) {
                stateHandles.push_back(stateInfo->columnFamilyHandle_);
            }
        }
        kvStateInformation_.clear();

        for (auto* stateHandle : stateHandles) {
            auto status = db->DestroyColumnFamilyHandle(stateHandle);
            EXPECT_TRUE(status.ok()) << "Failed to destroy state column family: " << status.ToString();
        }

        auto status = db->DestroyColumnFamilyHandle(result->getDefaultColumnFamilyHandle());
        EXPECT_TRUE(status.ok()) << "Failed to destroy default column family: " << status.ToString();
        status = db->Close();
        EXPECT_TRUE(status.ok()) << "Failed to close restored RocksDB: " << status.ToString();
        delete db;
    }

    KeyGroupRange keyGroupRange_{0, 0};
    std::filesystem::path dbPath_;
    std::shared_ptr<rocksdb::DBOptions> dbOptions_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation_;
    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::vector<std::string> events_;
    inline static std::atomic<unsigned long> nextDbPathId_{0};
};

} // namespace

// 看护 RocksDB compatible restore 先打开 DB，再进入 adaptor restore 生命周期并返回非空 DB。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, RestoreOpensDbBeforeDelegatedRestore)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    auto result = operation->restore();

    ASSERT_NE(result, nullptr);
    EXPECT_NE(result->getDb(), nullptr);
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "restore"}));
    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>{1U});
    closeRestoreResult(result);
}

// 两个 handle 必须分别交给 adaptor 校验，不能扁平为一次调用。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, MultipleHandlesWithSameStateAreValidatedIndependently)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle(), makeKeyedStateHandle()}, std::move(adaptor));

    auto result = operation->restore();

    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>({1U, 1U}));
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "validate", "restore"}));
    EXPECT_TRUE(adaptorPtr->actualIteratorHadNext_);
    closeRestoreResult(result);
}

// 后续 handle 校验失败必须发生在 openDB 和 restore 前，不能留下 RocksDB path 锁。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, SecondHandleValidationFailurePreventsRestore)
{
    EXPECT_CALL(*bridge_, readMetaData(_))
        .WillOnce(Return(std::vector<StateMetaInfoSnapshot>{makeMetaInfo()}))
        .WillOnce(Return(std::vector<StateMetaInfoSnapshot>{}));

    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->failOnEmptyMetadata_ = true;
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle(), makeKeyedStateHandle()}, std::move(adaptor));

    EXPECT_THROW(operation->restore(), std::runtime_error);
    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>({1U, 0U}));
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "validate"}));
    EXPECT_TRUE(kvStateInformation_.empty());
    expectPathCanBeOpenedAgain();
}

// 看护 bridge 缺失时在打开 RocksDB 前 fail-fast，原始异常和 construction registry 均保持明确终态。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, MissingBridgeDoesNotLockDbPath)
{
    auto operation =
        makeOperation({makeKeyedStateHandle()}, std::make_unique<RecordingRestoreAdaptor>(&events_), nullptr);

    expectThrowsWithMessage<std::invalid_argument>(
        [&] { operation->restore(); }, "RocksDB compatible restore requires OmniTaskBridge when state handles exist");
    EXPECT_TRUE(kvStateInformation_.empty());
    expectPathCanBeOpenedAgain();
}

// 看护 prepared adaptor 缺失时在打开 RocksDB 前 fail-fast，不能回退到 raw direct put。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, MissingPreparedAdaptorDoesNotLockDbPath)
{
    auto operation = makeOperation({makeKeyedStateHandle()}, nullptr);
    const auto adaptorInfo = makeAdaptorInfo();
    const auto expectedMessage = "RocksDB compatible restore requires prepared adaptor, type=" +
                                 std::to_string(static_cast<int>(adaptorInfo.type)) +
                                 ", builder/factory did not provide a prepared adaptor";

    expectThrowsWithMessage<std::runtime_error>([&] { operation->restore(); }, expectedMessage);
    EXPECT_TRUE(kvStateInformation_.empty());
    expectPathCanBeOpenedAgain();
}

// 看护 metadata validate 发生在 DB 打开和 restore 写入前，validate 失败不持有 lock。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, ValidateFailureDoesNotLockDbPath)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->throwOnValidate_ = true;
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    expectThrowsWithMessage<std::runtime_error>([&] { operation->restore(); }, "validate failed");
    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
    EXPECT_TRUE(kvStateInformation_.empty());
    expectPathCanBeOpenedAgain();
}

// 看护 compatible operation 不调用普通 full restore 的 key-group raw direct put 读取路径。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, RestoreDoesNotCallRawDirectPutPath)
{
    auto operation = makeOperation({makeKeyedStateHandle()}, std::make_unique<RecordingRestoreAdaptor>(&events_));

    EXPECT_CALL(*bridge_, setSavepointInputStreamOffset(_, _)).Times(0);
    EXPECT_CALL(*bridge_, ReadSavepointInputStream(_, _, _, _)).Times(0);
    auto result = operation->restore();
    closeRestoreResult(result);
}

// 看护成功路径返回 RocksDBRestoreResult，包含 DB 与 default column family。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, RestoreReturnsRocksDBRestoreResultOnSuccess)
{
    auto operation = makeOperation({makeKeyedStateHandle()}, std::make_unique<RecordingRestoreAdaptor>(&events_));

    auto result = operation->restore();

    ASSERT_NE(result, nullptr);
    EXPECT_NE(result->getDb(), nullptr);
    EXPECT_NE(result->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_EQ(result->getLastCompletedCheckpointId(), -1L);
    closeRestoreResult(result);
}

// 看护 adaptor restore 打开 DB 后失败时废弃 construction registry，并释放 native lock。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, RestoreFailureAbortsConstructionAndAllowsReopen)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->throwOnRestore_ = true;
    adaptor->onRestore_ = [&] {
        kvStateInformation_.emplace("partially-restored-state", std::make_shared<RocksDbKvStateInfo>());
    };
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    expectThrowsWithMessage<std::runtime_error>([&] { operation->restore(); }, "restore failed");
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "restore"}));
    EXPECT_TRUE(kvStateInformation_.empty());
    expectPathCanBeOpenedAgain();
}

// 看护 compatible full operation 不接受 incremental handle，类型不匹配时 fail-fast。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, CompatibleModeRejectsIncrementalHandle)
{
    KeyGroupRange keyGroupRange(0, 0);
    auto metaHandle = std::make_shared<ByteStreamStateHandle>("rocks-incremental-meta", std::vector<uint8_t>{1});
    auto incrementalHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
        UUID(),
        keyGroupRange,
        1L,
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        metaHandle);
    auto operation = makeOperation({incrementalHandle}, std::make_unique<RecordingRestoreAdaptor>(&events_));

    EXPECT_THROW(operation->restore(), std::runtime_error);
}

// 看护空 handles 只打开 DB 并返回结果，保持与 existing none/full 分支分离。
TEST_F(RocksDBCompatibleFullRestoreOperationTest, OperationKeepsExistingFullRestorePathSeparate)
{
    auto operation = makeOperation({}, nullptr);

    auto result = operation->restore();

    ASSERT_NE(result, nullptr);
    EXPECT_NE(result->getDb(), nullptr);
    EXPECT_TRUE(events_.empty());
    closeRestoreResult(result);
}
