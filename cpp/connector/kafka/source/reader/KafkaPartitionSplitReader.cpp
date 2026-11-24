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

#include "KafkaPartitionSplitReader.h"
#include <thread>
#include <utility>
#include <unordered_set>

namespace {
std::string topicPartitionKey(const RdKafka::TopicPartition* tp)
{
    if (tp == nullptr) {
        return "null";
    }
    return tp->topic() + "-" + std::to_string(tp->partition());
}
} // namespace

KafkaPartitionSplitReader::KafkaPartitionSplitReader(
    const std::unordered_map<std::string, std::string>& props, SourceReaderContext* context)
    : subtaskId(context->getSubTaskIndex())
{
    std::unordered_map<std::string, std::string> consumerProps = props;
    consumerProps["client.id"] = createConsumerClientId(props);

    bool commitOffsetsOnCheckpoint = true;
    auto it = props.find("commit.offsets.on.checkpoint");
    if (it != props.end() && it->second == "false") {
        commitOffsetsOnCheckpoint = false;
    }
    if (commitOffsetsOnCheckpoint) {
        consumerProps["enable.auto.commit"] = "false";
    }

    std::string errstr;
    consumer = new RdKafkaConsumer(consumerProps);
    // std::this_thread::sleep_for(std::chrono::seconds(2));
    if (!consumer) {
        throw std::runtime_error("Failed to create Kafka consumer: " + errstr);
    }
}

KafkaPartitionSplitReader::~KafkaPartitionSplitReader()
{
    close();
    delete consumer;
}

RecordsWithSplitIds<RdKafka::Message>* KafkaPartitionSplitReader::fetch()
{
    ConsumerRecords* consumerRecords;
    try {
        consumerRecords = consumer->poll(POLL_TIMEOUT);
    } catch (const std::exception& e) {
        consumerRecords = ConsumerRecords::EMPTY;
        auto recordsBySplits = new KafkaPartitionSplitRecords(consumerRecords);
        markEmptySplitsAsFinished(recordsBySplits);
        return recordsBySplits;
    }

    auto recordsBySplits = new KafkaPartitionSplitRecords(consumerRecords);
    std::vector<RdKafka::TopicPartition*> finishedPartitions;
    std::vector<RdKafka::TopicPartition*> recordsPartitions;
    consumerRecords->partitions(recordsPartitions);
    for (auto& tp : recordsPartitions) {
        long stoppingOffset = getStoppingOffset(tp);
        auto recordsFromPartition = consumerRecords->records(tp);
        if (!recordsFromPartition.empty()) {
            auto lastRecord = recordsFromPartition.back();
            if (lastRecord->offset() >= stoppingOffset - 1) {
                recordsBySplits->setPartitionStoppingOffset(tp, stoppingOffset);
                finishedPartitions.push_back(tp);
                recordsBySplits->addFinishedSplit(KafkaPartitionSplit::toSplitId(tp));
            }
        }
    }

    markEmptySplitsAsFinished(recordsBySplits);

    if (!finishedPartitions.empty()) {
        unassignPartitions(finishedPartitions);
    }
    return recordsBySplits;
}

void KafkaPartitionSplitReader::handleSplitsChanges(const std::vector<KafkaPartitionSplit*>& splitsChange)
{
    std::vector<RdKafka::TopicPartition*> newPartitionAssignments;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStoppingAtLatest;
    std::vector<std::shared_ptr<RdKafka::TopicPartition>> partitionsStoppingAtCommitted;

    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);

    std::unordered_set<std::string> assignmentKeys;
    newPartitionAssignments.reserve(currentAssignment.size() + splitsChange.size());
    for (auto* tp : currentAssignment) {
        if (tp == nullptr) {
            continue;
        }
        const std::string splitId = topicPartitionKey(tp);
        if (assignmentKeys.insert(splitId).second) {
            newPartitionAssignments.push_back(tp);
        }
    }

    size_t acceptedNewSplits = 0;
    for (const auto& s : splitsChange) {
        if (s == nullptr) {
            continue;
        }
        auto tp = s->getTopicPartition();
        if (tp == nullptr) {
            continue;
        }
        const std::string splitId = s->splitId();
        if (!assignmentKeys.insert(splitId).second) {
            continue;
        }

        acceptedNewSplits++;
        setStartingOffsetForAssignment(s);
        newPartitionAssignments.push_back(tp.get());
        parseStoppingOffsets(s, partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
    }

    if (acceptedNewSplits == 0) {
        for (auto* tp : currentAssignment) {
            delete tp;
        }
        return;
    }

    consumer->assign(newPartitionAssignments);
    for (auto* tp : currentAssignment) {
        delete tp;
    }

    acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
    removeEmptySplits();

    maybeLogSplitChangesHandlingResult(splitsChange);
}

void KafkaPartitionSplitReader::wakeUp()
{
}

void KafkaPartitionSplitReader::close()
{
    consumer->close();
}

void KafkaPartitionSplitReader::markEmptySplitsAsFinished(KafkaPartitionSplitRecords* recordsBySplits)
{
    if (!emptySplits.empty()) {
        for (const auto& split : emptySplits) {
            recordsBySplits->finishedSplits().insert(split);
        }
        emptySplits.clear();
    }
}

std::string KafkaPartitionSplitReader::createConsumerClientId(const std::unordered_map<std::string, std::string>& props)
{
    auto it = props.find("client.id.prefix");
    std::string prefix = (it != props.end()) ? it->second : "";
    return prefix + "-" + std::to_string(subtaskId);
}

void KafkaPartitionSplitReader::setStartingOffsetForAssignment(KafkaPartitionSplit* split)
{
    if (split == nullptr) {
        return;
    }

    const std::shared_ptr<RdKafka::TopicPartition> tp = split->getTopicPartition();
    if (!tp) {
        return;
    }

    const int64_t startingOffset = split->getStartingOffset();
    int64_t assignmentOffset = startingOffset;
    if (startingOffset == KafkaPartitionSplit::EARLIEST_OFFSET) {
        assignmentOffset = RdKafka::Topic::OFFSET_BEGINNING;
    } else if (startingOffset == KafkaPartitionSplit::LATEST_OFFSET) {
        assignmentOffset = RdKafka::Topic::OFFSET_END;
    } else if (startingOffset == KafkaPartitionSplit::COMMITTED_OFFSET) {
        assignmentOffset = RdKafka::Topic::OFFSET_STORED;
    }

    tp->set_offset(assignmentOffset);
}

void KafkaPartitionSplitReader::parseStoppingOffsets(
    KafkaPartitionSplit* split,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted)
{
    const std::shared_ptr<RdKafka::TopicPartition> tp = split->getTopicPartition();
    auto stoppingOffset = split->getStoppingOffset();
    if (stoppingOffset >= 0) {
        stoppingOffsets[tp.get()] = stoppingOffset;
    } else if (stoppingOffset == KafkaPartitionSplit::LATEST_OFFSET) {
        partitionsStoppingAtLatest.push_back(tp);
    } else if (stoppingOffset == KafkaPartitionSplit::COMMITTED_OFFSET) {
        partitionsStoppingAtCommitted.push_back(tp);
    }
}

void KafkaPartitionSplitReader::acquireAndSetStoppingOffsets(
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtLatest,
    std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitionsStoppingAtCommitted)
{
    consumer->endOffsets(partitionsStoppingAtLatest);
    for (const auto& tp : partitionsStoppingAtLatest) {
        stoppingOffsets[tp.get()] = tp->offset();
    }
    if (!partitionsStoppingAtCommitted.empty()) {
        consumer->committed(partitionsStoppingAtCommitted);
        for (const auto& tp : partitionsStoppingAtCommitted) {
            stoppingOffsets[tp.get()] = tp->offset();
        }
    }
}

void KafkaPartitionSplitReader::removeEmptySplits()
{
    std::vector<RdKafka::TopicPartition*> emptyPartitions;
    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);
    consumer->position(currentAssignment);
    for (auto* tp : currentAssignment) {
        if (tp->offset() >= getStoppingOffset(tp)) {
            emptyPartitions.push_back(tp);
        }
    }
    if (!emptyPartitions.empty()) {
        for (auto* tp : emptyPartitions) {
            emptySplits.insert(KafkaPartitionSplit::toSplitId(tp));
        }
        unassignPartitions(emptyPartitions);
    }
    for (auto* tp : currentAssignment) {
        delete tp;
    }
}

void KafkaPartitionSplitReader::maybeLogSplitChangesHandlingResult(
    const std::vector<KafkaPartitionSplit*>& splitsChange)
{
}

void KafkaPartitionSplitReader::unassignPartitions(const std::vector<RdKafka::TopicPartition*>& partitionsToUnassign)
{
    std::vector<RdKafka::TopicPartition*> currentAssignment;
    consumer->assignment(currentAssignment);

    std::unordered_set<std::string> unassignSet;
    for (auto* tp : partitionsToUnassign) {
        if (tp != nullptr) {
            unassignSet.insert(topicPartitionKey(tp));
        }
    }

    std::vector<RdKafka::TopicPartition*> newAssignment;
    for (auto tp : currentAssignment) {
        if (tp != nullptr && unassignSet.find(topicPartitionKey(tp)) == unassignSet.end()) {
            newAssignment.push_back(tp);
        }
    }
    consumer->assign(newAssignment);
    for (auto* tp : currentAssignment) {
        delete tp;
    }
}

long KafkaPartitionSplitReader::getStoppingOffset(RdKafka::TopicPartition* tp)
{
    auto it = stoppingOffsets.find(tp);
    return (it != stoppingOffsets.end()) ? it->second : std::numeric_limits<long>::max();
}

void KafkaPartitionSplitReader::commitOffsets(
    const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsets)
{
    consumer->commitOffsets(offsets);
}
