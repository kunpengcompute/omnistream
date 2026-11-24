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

#ifndef OMNISTREAM_ROCKSDBINCREMENTALCHECKPOINTUTILS_H
#define OMNISTREAM_ROCKSDBINCREMENTALCHECKPOINTUTILS_H

#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "runtime/state/CompositeKeySerializationUtils.h"

#include <rocksdb/db.h>

class RocksDBIncrementalCheckpointUtils {
public:
    /**
     * Score for evaluating state handles during recovery initialization
     */
    class Score {
    public:
        Score(int intersectGroupRange, double overlapFraction)
            : intersectGroupRange_(intersectGroupRange),
              overlapFraction_(overlapFraction)
        {
        }

        int getIntersectGroupRange() const
        {
            return intersectGroupRange_;
        }
        double getOverlapFraction() const
        {
            return overlapFraction_;
        }

        bool operator>(const Score& other) const
        {
            if (intersectGroupRange_ != other.intersectGroupRange_) {
                return intersectGroupRange_ > other.intersectGroupRange_;
            }
            return overlapFraction_ > other.overlapFraction_;
        }

    private:
        int intersectGroupRange_;
        double overlapFraction_;
    };

    /**
     * Evaluates state handle's "score" regarding to the target range
     */
    static Score stateHandleEvaluator(
        const KeyedStateHandle& stateHandle, const KeyGroupRange& targetKeyGroupRange, double overlapFractionThreshold)
    {
        const auto handleKeyGroupRange = stateHandle.GetKeyGroupRange();
        const auto intersectGroup = handleKeyGroupRange.getIntersection(targetKeyGroupRange);
        const double overlapFraction =
            static_cast<double>(intersectGroup->getNumberOfKeyGroups()) / handleKeyGroupRange.getNumberOfKeyGroups();
        if (overlapFraction < overlapFractionThreshold) {
            return Score(INT_MIN, -1.0);
        }
        return Score(intersectGroup->getNumberOfKeyGroups(), overlapFraction);
    }

    /**
     * Clips the DB instance according to the target key group range
     */
    static void clipDBWithKeyGroupRange(
        rocksdb::DB* db,
        const std::vector<rocksdb::ColumnFamilyHandle*>& columnFamilyHandles,
        const KeyGroupRange& targetKeyGroupRange,
        const KeyGroupRange& currentKeyGroupRange,
        int keyGroupPrefixBytes)
    {
        std::vector<uint8_t> beginKeyGroupBytes(keyGroupPrefixBytes);
        std::vector<uint8_t> endKeyGroupBytes(keyGroupPrefixBytes);

        if (currentKeyGroupRange.getStartKeyGroup() < targetKeyGroupRange.getStartKeyGroup()) {
            CompositeKeySerializationUtils::serializeKeyGroup(
                currentKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);
            CompositeKeySerializationUtils::serializeKeyGroup(targetKeyGroupRange.getStartKeyGroup(), endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }

        if (currentKeyGroupRange.getEndKeyGroup() > targetKeyGroupRange.getEndKeyGroup()) {
            CompositeKeySerializationUtils::serializeKeyGroup(
                targetKeyGroupRange.getEndKeyGroup() + 1, beginKeyGroupBytes);
            CompositeKeySerializationUtils::serializeKeyGroup(
                currentKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }
    }
    /**
     * Checks if bytes are before prefixBytes in lex order
     */
    static bool beforeThePrefixBytes(const rocksdb::Slice& bytes, const rocksdb::Slice& prefixBytes)
    {
        const size_t prefixLength = prefixBytes.size();
        for (size_t i = 0; i < prefixLength; ++i) {
            int r = prefixBytes[i] - bytes[i];
            if (r != 0) {
                return r > 0;
            }
        }
        return false;
    }

    /**
     * Chooses the best state handle for initial DB initialization
     */
    static std::shared_ptr<KeyedStateHandle> chooseTheBestStateHandleForInitial(
        const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
        const KeyGroupRange& targetKeyGroupRange,
        double overlapFractionThreshold)
    {
        std::shared_ptr<KeyedStateHandle> bestStateHandle = nullptr;
        Score bestScore = Score(INT_MIN, -1.0);

        for (const auto& rawStateHandle : restoreStateHandles) {
            Score handleScore = stateHandleEvaluator(*rawStateHandle, targetKeyGroupRange, overlapFractionThreshold);
            if (handleScore > bestScore) {
                bestStateHandle = rawStateHandle;
                bestScore = handleScore;
            }
        }

        return bestStateHandle;
    }

private:
    /**
     * Deletes range [beginKeyBytes, endKeyBytes) from the DB
     */
    static void deleteRange(
        rocksdb::DB* db,
        const std::vector<rocksdb::ColumnFamilyHandle*>& columnFamilyHandles,
        const std::vector<uint8_t>& beginKeyBytes,
        const std::vector<uint8_t>& endKeyBytes)
    {
        rocksdb::Slice beginSlice(reinterpret_cast<const char*>(beginKeyBytes.data()), beginKeyBytes.size());
        rocksdb::Slice endSlice(reinterpret_cast<const char*>(endKeyBytes.data()), endKeyBytes.size());

        for (auto handle : columnFamilyHandles) {
            rocksdb::Status status = db->DeleteRange(rocksdb::WriteOptions(), handle, beginSlice, endSlice);
            if (!status.ok()) {
                throw std::runtime_error("RocksDB delete range failed: " + status.ToString());
            }
        }
    }
};
#endif // OMNISTREAM_ROCKSDBINCREMENTALCHECKPOINTUTILS_H
