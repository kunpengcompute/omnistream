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

#ifndef OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H

#include <utility>
#include <runtime/io/network/api/serialization/RecordDeserializer.h>
#include <runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h>
#include <runtime/plugable/DeserializationDelegate.h>
#include <runtime/plugable/NonReusingDeserializationDelegate.h>
#include <streaming/runtime/streamrecord/StreamElementSerializer.h>

#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniStreamTaskInput.h"
#include "buffer/NetworkBuffer.h"
#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "partition/consumer/InputChannelInfo.h"
#include "partition/consumer/InputGate.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkOutput.h"
#include "typeutils/TypeSerializer.h"
#include "runtime/io/checkpointing/CheckpointedInputGate.h"
#include "runtime/event/EndOfChannelStateEvent.h"
#include "runtime/event/InnerRecoverEvent.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "runtime/watermark/StatusWatermarkValve.h"

namespace omnistream {
class OmniAbstractStreamTaskNetworkInput : public OmniStreamTaskInput {
public:
    OmniAbstractStreamTaskNetworkInput(
        int64_t inputIndex,
        std::shared_ptr<CheckpointedInputGate> inputGate,
        int taskType,
        TypeSerializer* inputSerializer,
        std::vector<long>& channelInfos,
        std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> recordDeserializers,
        int64_t checkpointInterval)
        : recordDeserializers(std::move(recordDeserializers)),
          inputIndex(inputIndex),
          inputGate(inputGate),
          taskType(taskType),
          inputFormat_(taskType == 1 ? inputGate->getInputFormat() : InputFormat::FLINK_ROW),
          currentRecordDeserializer(nullptr),
          output_(nullptr),
          statusWatermarkValve_(this->inputGate->GetNumberOfInputChannels()),
          checkpointInterval(checkpointInterval)
    {
        INFO_RELEASE("create OmniAbstractStreamTaskNetworkInput, task type is:" << taskType);
        inSerializer = inputSerializer;
        deserializationDelegate_ = std::make_unique<NonReusingDeserializationDelegate>(
            new datastream::StreamElementSerializer(inputSerializer));
        rowCount = 0;
        maxRowCount = 1000;
        timeout = 1000;
        running_.exchange(true);
    }

    OmniAbstractStreamTaskNetworkInput(
        int64_t inputIndex,
        std::shared_ptr<CheckpointedInputGate> inputGate,
        int taskType,
        TypeSerializer* inputSerializer,
        std::vector<long>& channelInfos,
        int64_t checkpointInterval)
        : inputIndex(inputIndex),
          inputGate(inputGate),
          taskType(taskType),
          inputFormat_(taskType == 1 ? inputGate->getInputFormat() : InputFormat::FLINK_ROW),
          currentRecordDeserializer(nullptr),
          output_(nullptr),
          statusWatermarkValve_(this->inputGate->GetNumberOfInputChannels()),
          checkpointInterval(checkpointInterval)
    {
        INFO_RELEASE("create OmniAbstractStreamTaskNetworkInput, task type is:" << taskType);
        inSerializer = inputSerializer;
        deserializationDelegate_ = std::make_unique<NonReusingDeserializationDelegate>(
            new datastream::StreamElementSerializer(inputSerializer));
        recordDeserializers = getRecordDeserializers(channelInfos);
        rowCount = 0;
        maxRowCount = 1000;
        timeout = 1000;
        running_.exchange(true);
    }

    DataInputStatus emitNext(OmniPushingAsyncDataInput::OmniDataOutput* output) override
    {
        // temp fix for taskType, will be removed in the future (taskType should not be set in Runtime)
        output->setTaskType(taskType);

        if (taskType == 1) {
            if (inputFormat_ == InputFormat::FLINK_ROW) {
                if (!isStartTimer) {
                    INFO_RELEASE("Start timer thread");
                    timer_thread_ = std::thread(&OmniAbstractStreamTaskNetworkInput::timerThread, this);
                    isStartTimer = true;
                }
                output_ = output;
                return processForSQLFromOriginal(output);
            } else {
                return processForSQL(output);
            }
        } else if (taskType == 2) {
            return processForDataStream(output);
        } else {
            INFO_RELEASE("Unknown taskType: " << taskType);
        }
    }

    void timerThread()
    {
        while (running_) {
            std::unique_lock<std::mutex> lock(mutex_);
            if (cv_.wait_for(lock, std::chrono::seconds(1), [this]() { return !running_ || rowList.empty(); })) {
                if (!running_) break;
            } else {
                if (!rowList.empty()) {
                    lock.unlock();
                    INFO_RELEASE("Raw to Native, triggering by schedule");
                    emitCurrentBatch(output_);
                }
            }
        }
        INFO_RELEASE("timer thread end");
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        // no inputGate no output

        if (taskType == 1 && checkpointInterval == -1) {
            return AVAILABLE;
        } else {
            if (currentRecordDeserializer != nullptr) {
                return AVAILABLE;
            }
            return inputGate->GetAvailableFuture();
        }
    }
    std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> getRecordDeserializers(
        std::vector<long>& channelInfos)
    {
        std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> recordDeserializers =
            std::make_unique<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>>();
        for (size_t i = 0; i < channelInfos.size(); i++) {
            LOG("getRecordDeserializers channelInfo " << i);
            auto deserializer = std::make_unique<SpillingAdaptiveSpanningRecordDeserializer>();
            recordDeserializers->emplace(channelInfos.at(i), std::move(deserializer));
        }
        return recordDeserializers;
    }

    virtual RecordDeserializer* getActiveSerializer(long channelInfo)
    {
        auto it = recordDeserializers->find(channelInfo);
        if (it == recordDeserializers->end()) {
            INFO_RELEASE("Error: ChannelInfo not found in recordDeserializers:" << channelInfo);
            THROW_RUNTIME_ERROR("ChannelInfo not found in recordDeserializers:" + std::to_string(channelInfo));
        }
        return it->second.get();
    }

    DataInputStatus processBufferOrEventOptForSQL(
        OmniPushingAsyncDataInput::OmniDataOutput* output, BufferOrEvent* bufferOrEvent)
    {
        isLastValueNull = false;
        NullValueCount = 0;

        LOG(">>>>> bufferOrEventOpt bufferOrEvent" + std::to_string(reinterpret_cast<int64_t>(bufferOrEvent)));
        if (bufferOrEvent->isBuffer()) {
            auto buff = reinterpret_cast<ObjectBuffer*>(bufferOrEvent->getBuffer());
            lastChannel_ = bufferOrEvent->getChannelInfo();

            auto size = buff->GetSize();
            auto objSegment = buff->GetObjectSegment();
            auto offset = buff->GetOffset();
            LOG(">>>>object segment is " << std::to_string(reinterpret_cast<long>(objSegment)));
            LOG(">>>>>buffer size is " << size << " buffer offset is " << offset);

            LOG("===================start output=======================");
            for (int64_t index = offset; index < offset + size; index++) {
                StreamElement* object = objSegment->getObject(index);
                int tag = static_cast<int>(object->getTag());
                LOG("OmniAbstractStreamTaskNetworkInput tag: " << tag << " channelIndex: "
                                                               << lastChannel_.getInputChannelIdx());
                if (object->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                    object->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
                    auto record = static_cast<StreamRecord*>(object);
                    auto vectorBatch = static_cast<VectorBatch*>(record->getValue());
                    size_t row_cnt = vectorBatch->GetRowCount();
                    numberOfRow += row_cnt;

                    output->emitRecord(reinterpret_cast<StreamRecord*>(object));
                } else if (object->getTag() == StreamElementTag::TAG_WATERMARK) {
                    auto watermark = std::unique_ptr<Watermark>(reinterpret_cast<Watermark*>(object));
                    statusWatermarkValve_.inputWatermark(watermark.get(), lastChannel_.getInputChannelIdx(), output);
                } else if (object->getTag() == StreamElementTag::TAG_STREAM_STATUS) {
                    statusWatermarkValve_.inputWatermarkStatus(
                        reinterpret_cast<WatermarkStatus*>(object), lastChannel_.getInputChannelIdx(), output);
                } else {
                    LOG("Bypass the tag for now: " << tag);
                }
            }
            // more avaiable means there could be more data come in
            buff->RecycleBuffer();
            delete buff; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
            buff = nullptr;

            // more avaiable means there could be more data come in
            return DataInputStatus::MORE_AVAILABLE;
        } else {
            // we got event
            // so far, we only knows
            DataInputStatus status = processEvent(bufferOrEvent);
            return status;
        }
    }

    DataInputStatus processForSQL(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        while (true) {
            auto bufferOrEvent = inputGate->PollNext();
            if (bufferOrEvent) {
                DataInputStatus status = processBufferOrEventOptForSQL(output, bufferOrEvent);
                delete bufferOrEvent;
                return status;
            } else {
                if (isLastValueNull) {
                    NullValueCount++;
                }
                isLastValueNull = true;
                if (NullValueCount % 100 == 1) {
                    // LOG(">>>>> bufferOrEventOpt has NO value, found continousely " << NullValueCount)
                }
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    void processBufferForDataStreamAndSQLFromOriginal(BufferOrEvent* bufferOrEvent)
    {
        auto buffer = static_cast<ReadOnlySlicedNetworkBuffer*>(bufferOrEvent->getBuffer());
        auto inputChannelInfo = bufferOrEvent->getChannelInfo();
        currentRecordDeserializer = getActiveSerializer(inputChannelInfo.getComplexId());
        if (currentRecordDeserializer == nullptr) {
            INFO_RELEASE("currentRecordDeserializer has already been released");
            THROW_LOGIC_EXCEPTION("currentRecordDeserializer has already been released");
        }
        currentRecordDeserializer->SetNextBuffer(buffer);
    }

    DataInputStatus processFullRecordForDataStream(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        auto* element = static_cast<StreamElement*>(deserializationDelegate_->getInstance());
        if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            output->emitWatermark(reinterpret_cast<Watermark*>(element));
        } else if (element->getTag() == StreamElementTag::TAG_STREAM_STATUS) {
            output->emitWatermarkStatus(reinterpret_cast<WatermarkStatus*>(element));
        } else {
            processElement(element, output);
        }
        return DataInputStatus::MORE_AVAILABLE;
    }

    DataInputStatus processForDataStream(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        try {
            while (true) {
                if (currentRecordDeserializer != nullptr) {
                    DeserializationResult& result = currentRecordDeserializer->getNextRecord(*deserializationDelegate_);
                    if (unlikely(result.isBufferConsumed())) {
                        currentRecordDeserializer = nullptr;
                    }

                    if (likely(result.isFullRecord())) {
                        return processFullRecordForDataStream(output);
                        // continue;
                    }
                }
                auto bufferOrEvent = inputGate->PollNext();
                if (bufferOrEvent) {
                    if (bufferOrEvent->isBuffer()) {
                        processBufferForDataStreamAndSQLFromOriginal(bufferOrEvent);
                        delete bufferOrEvent;
                    } else {
                        DataInputStatus status = processEvent(bufferOrEvent);
                        delete bufferOrEvent;
                        return status;
                    }
                } else {
                    return DataInputStatus::NOTHING_AVAILABLE;
                }
            }
        } catch (const std::exception& e) {
            INFO_RELEASE("processForDataStream exception:" << e.what());
            throw std::runtime_error(e.what());
        }
    }

    DataInputStatus processForSQLFromOriginal(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        while (true) {
            if (currentRecordDeserializer != nullptr) {
                DeserializationResult& result = currentRecordDeserializer->getNextRecord(*deserializationDelegate_);

                if (unlikely(result.isBufferConsumed())) {
                    LOG("isBufferConsumed: do we really buffer consumed?!!!");
                    currentRecordDeserializer = nullptr;
                }

                if (likely(result.isFullRecord())) {
                    return processFullRecordForSQLFromOriginal(output);
                }
            }

            auto bufferOrEvent = inputGate->PollNext();
            if (bufferOrEvent) {
                if (bufferOrEvent->isBuffer()) {
                    processBufferForDataStreamAndSQLFromOriginal(bufferOrEvent);
                    delete bufferOrEvent;
                } else {
                    DataInputStatus status = processEvent(*bufferOrEvent, output);
                    delete bufferOrEvent;
                    return status;
                }
            } else {
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    DataInputStatus processFullRecordForSQLFromOriginal(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        auto* element =
            reinterpret_cast<StreamElement*>(deserializationDelegate_->getInstance()); // 这里面包装的是BinaryRowData
        if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            output->emitWatermark(reinterpret_cast<Watermark*>(element));
        } else {
            auto record = reinterpret_cast<StreamRecord*>(element);
            auto row = reinterpret_cast<BinaryRowData*>(record->getValue());

            // 1.If rowList is empty, reset the batch start time
            if (rowList.empty()) {
                batchStartTime = std::chrono::steady_clock::now();
            }

            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (rowList.empty()) {
                    batchStartTime = std::chrono::steady_clock::now();
                }

                // 2.Push the row data into list
                auto newRow = reinterpret_cast<BinaryRowData*>(row->copy());
                rowList.push_back(newRow);
                LOG("push in a record, size is " << rowList.size());
                rowCount++;
                numberOfRow++;
            }

            // 3.Calculate the elapsed time
            auto currentTime = std::chrono::steady_clock::now();
            auto elapsedMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - batchStartTime).count();
            // 4.If the number of records reaches the maximum capacity is reached
            if (rowCount >= maxRowCount || elapsedMs >= timeout) {
                LOG("Reach the maximum capacity or timeout, start to generate VectorBatch and send to output");
                // 3. Convert the rowList to VectorBatch, send to output, and clear the state
                emitCurrentBatch(output);
            }
        }
        return DataInputStatus::MORE_AVAILABLE;
    }

    void emitCurrentBatch(OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        if (rowCount == 0) {
            return;
        }
        // Convert the rowdata list to VectorBatch
        StreamRecord* batchRecord = nullptr;
        const std::vector<std::string>& inputTypes =
            reinterpret_cast<BinaryRowDataSerializer*>(inSerializer)->getInputTypes();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            omnistream::VectorBatch* resultBatch = createOutputBatch(rowList, inputTypes);
            batchRecord = new StreamRecord(resultBatch);
            for (BinaryRowData* row : rowList) {
                delete row;
            }
            rowList.clear();
            rowCount = 0;
            cv_.notify_one();
        }
        output->emitRecord(batchRecord);

        batchStartTime = std::chrono::steady_clock::now();
    }

    omnistream::VectorBatch* createOutputBatch(
        std::vector<BinaryRowData*> collectedRows, const std::vector<std::string>& inputTypes)
    {
        INFO_RELEASE("Start to createOutputBatch");
        int numColumns = inputTypes.size();
        auto inputRowType = new std::vector<omniruntime::type::DataTypeId>;
        for (const auto& typeStr : inputTypes) {
            inputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
        int numRows = collectedRows.size();
        INFO_RELEASE("collectedRows: " << collectedRows.size());
        auto* outputBatch = new omnistream::VectorBatch(numRows);
        for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
            switch (inputRowType->at(colIndex)) {
                case DataTypeId::OMNI_LONG:
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    setLong(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_INT: {
                    setInt(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    setDouble(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_BOOLEAN: {
                    setBool(outputBatch, numRows, colIndex, rowList);
                    break;
                }
                case DataTypeId::OMNI_CHAR:
                case DataTypeId::OMNI_VARCHAR: {
                    setString(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL64: {
                    setDecimal64(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL128: {
                    setDecimal128(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                default: {
                    delete inputRowType;
                    throw std::runtime_error("Unsupported column type in inputRow");
                }
            }
        }
        delete inputRowType;
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
        }
        return outputBatch;
    }

    void setInt(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setLong(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setDecimal64(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows, DataTypeId::OMNI_DECIMAL64);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setDecimal128(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<Decimal128>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getDecimal128(colIndex, 0));
            }
        }
        outputBatch->Append(vector);
    }

    void setString(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
        VarcharVector* vector = new VarcharVector(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                auto value = std::string(collectedRows[rowIndex]->getStringView(colIndex));
                omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(
                    vector, rowIndex, (void*)&value);
            }
        }
        outputBatch->Append(vector);
    }

    void setDouble(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<double>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setBool(
        omniruntime::vec::VectorBatch* outputBatch,
        int numRows,
        int colIndex,
        std::vector<BinaryRowData*> collectedRows)
    {
        auto* vector = new omniruntime::vec::Vector<bool>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getBool(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    int getInputIndex() override
    {
        return static_cast<int>(inputIndex);
    }

    void close() override
    {
        running_.exchange(false);
        INFO_RELEASE("OmniAbstractStreamTaskNetworkInput received numberOfRow: " << numberOfRow);
    }

    std::shared_ptr<CompletableFutureV2<void>> PrepareSnapshot(
        std::shared_ptr<ChannelStateWriter> writer, long checkpointId) override
    {
        LOG("Network prepare snapshot, checkpointId: " << checkpointId);
        for (const auto& pair : *recordDeserializers) {
            std::vector<InputChannelInfo> channelInfofos = inputGate->GetChannelInfos();
            std::unordered_map<long, InputChannelInfo> map;
            for (auto& item : channelInfofos) {
                map.emplace(item.getComplexId(), item);
            }
            try {
                std::vector<omnistream::Buffer*> buffers = (pair.second)->GetUnconsumedBuffer();
                int bufferSize = buffers.size();
                if (bufferSize > 0) {
                    writer->AddInputData(
                        checkpointId, map[pair.first], ChannelStateWriter::sequenceNumberUnknown, buffers);
                } else {
                    LOG_DEBUG(" PrepareSnapshot buffers is null ");
                }
            } catch (const std::exception& e) {
                throw std::runtime_error("Error: " + std::string(e.what()));
            }
        }

        auto f = inputGate->GetAllBarriersReceivedFuture(checkpointId);
        LOG("PrepareSnapshot cp=" << checkpointId << " future_ptr=" << f);
        return f;
    }

protected:
    int64_t inputIndex;
    std::shared_ptr<CheckpointedInputGate> inputGate;
    std::atomic<long> numberOfRow{0};

    void processElement(StreamElement* recordOrMark, OmniDataOutput* output)
    {
        output->emitRecord(static_cast<StreamRecord*>(recordOrMark));
    }

    virtual DataInputStatus processEvent(BufferOrEvent* bufferOrEvent)
    {
        std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
        if (dynamic_cast<EndOfData*>(event.get())) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->HasReceivedEndOfData()) {
                INFO_RELEASE("received a EndOfData event!");
                return DataInputStatus::END_OF_DATA;
            }
        } else if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->IsFinished()) {
                INFO_RELEASE("received a EndOfPartitionEvent event!");
                return DataInputStatus::END_OF_INPUT;
            }
        } else if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
            INFO_RELEASE(
                "received EndOfChannelStateEvent start, inputIndex=" << inputIndex << ", channel="
                                                                     << bufferOrEvent->getChannelInfo().toString());
            if (inputGate->AllChannelsRecovered()) {
                INFO_RELEASE("received EndOfChannelStateEvent end, inputIndex=" << inputIndex);
                return DataInputStatus::END_OF_RECOVERY;
            }
        } else if (dynamic_cast<InnerRecoverEvent*>(event.get())) {
            INFO_RELEASE("received a inner recovery event start");
            if (inputGate->AllInnerChannelsRecovered()) {
                INFO_RELEASE("received a inner recovery event end");
                return DataInputStatus::INNER_RECOVER;
            }
        }
        // by default,continue the data processing
        return DataInputStatus::MORE_AVAILABLE;
    }

    // Specifically for SQL from original task
    DataInputStatus processEvent(BufferOrEvent& bufferOrEvent, OmniPushingAsyncDataInput::OmniDataOutput* output)
    {
        std::shared_ptr<AbstractEvent> event = bufferOrEvent.getEvent();
        if (dynamic_cast<EndOfData*>(event.get())) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->HasReceivedEndOfData()) {
                // Which means reach the end of Data, if the rowList still remains data, create the last vectorbatch and
                // send to output
                emitCurrentBatch(output);
                return DataInputStatus::END_OF_DATA;
            }
        } else if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->IsFinished()) {
                return DataInputStatus::END_OF_INPUT;
            }
        } else if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
            INFO_RELEASE("received a end of recovery event start");
            if (inputGate->AllChannelsRecovered()) {
                INFO_RELEASE("received a end of recovery event end");
                return DataInputStatus::END_OF_RECOVERY;
            }
        }
        // by default,continue the data processing
        return DataInputStatus::MORE_AVAILABLE;
    }

protected:
    std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> recordDeserializers;
    // for troubleshooting
    int NullValueCount = 0;
    bool isLastValueNull = false;
    int taskType;
    const InputFormat inputFormat_;
    RecordDeserializer* currentRecordDeserializer;
    std::unique_ptr<DeserializationDelegate> deserializationDelegate_;
    TypeSerializer* inSerializer;
    // When the current SQL task has a Java task as its upstream,
    // maintain the number of rows accumulated for generating VectorBatch.
    int rowCount;
    // Maximum number of rows supported by VectorBatch
    int maxRowCount;
    // Maximum timeout for accumulating VectorBatch(ms)
    int timeout;
    // List of BinaryRowData to be accumulated, only init in SQLFromOriginal case
    std::vector<BinaryRowData*> rowList;
    std::chrono::steady_clock::time_point batchStartTime;

    // Valve that controls how watermarks and watermark statuses are forwarded.
    StatusWatermarkValve<OmniPushingAsyncDataInput::OmniDataOutput> statusWatermarkValve_;
    InputChannelInfo lastChannel_;

    // timerThread相关
    std::atomic<bool> running_{false};
    std::mutex mutex_;           // 保护缓冲区的互斥锁
    std::condition_variable cv_; // 用于线程同步的条件变量
    std::thread timer_thread_;   // 定时器线程
    OmniPushingAsyncDataInput::OmniDataOutput* output_;
    bool isStartTimer = false;

    int64_t checkpointInterval = -1;
};
} // namespace omnistream

#endif // OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
