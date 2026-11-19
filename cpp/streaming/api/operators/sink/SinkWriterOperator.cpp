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

#include "SinkWriterOperator.h"
#include "streaming/api/operators/sink/SinkV1WriterCommittableSerializer.h"

namespace {
std::string streamingCommitterRawStatesName = "streaming_committer_raw_states";
BytePrimitiveArraySerializer streamingCommitterRawStatesSerializer(nullptr);
} // namespace

// 静态成员初始化
ListStateDescriptor<std::vector<uint8_t>> SinkWriterOperator::STREAMING_COMMITTER_RAW_STATES_DESC(
    streamingCommitterRawStatesName, &streamingCommitterRawStatesSerializer);

// createInitContext方法实现
template <typename K>
InitContextImpl<K>* SinkWriterOperator::createInitContext(std::optional<uint64_t> restoredCheckpointId)
{
    return new InitContextImpl<K>(
        this->runtimeContext,
        reinterpret_cast<ProcessingTimeServiceImpl*>(this->processingTimeService),
        restoredCheckpointId);
}

SinkWriterOperator::SinkWriterOperator(KafkaSink* kafkaSink, const nlohmann::json& config)
    : kafkaSink(kafkaSink),
      endOfInput(false),
      description(config),
      isDataStream(!config["batch"])
{
    if (config["batch"]) {
        inputTypes = config["inputTypes"].get<std::vector<std::basic_string<char>>>();
    }

    isDataStream = !description["batch"];

    // statefulSink初始化状态处理句柄
    writerStateHandler = new KafkaSinkWriterStateHandler(kafkaSink);

    // TwoPhaseCommittingSink
    emitDownstream = true;
    committableSerializer = kafkaSink->getCommittableSerializer();
}

void SinkWriterOperator::open()
{
}

void SinkWriterOperator::close()
{
    if (closed_) {
        return;
    }
    closed_ = true;
    INFO_RELEASE("savepoint: SinkWriterOperator close START");
    if (!endOfInput && sinkWriter != nullptr) {
        EndInput();
    }
    delete writerStateHandler;
    writerStateHandler = nullptr;
    sinkWriter = nullptr;
    delete committableSerializer;
    committableSerializer = nullptr;
    delete kafkaSink;
    kafkaSink = nullptr;
    // todo 补全close逻辑
    // AbstractStreamOperator<void*>::close();
    INFO_RELEASE("savepoint: SinkWriterOperator close END");
}

RowData* SinkWriterOperator::getOutputEntireRow(omnistream::VectorBatch* vecBatch, int rowId)
{
    int colsCount = vecBatch->GetVectorCount();
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(colsCount);
    for (int32_t i = 0; i < colsCount; i++) {
        int pos = i;
        if (inputTypes[i] == "BIGINT" || inputTypes[i].find("TIMESTAMP") != std::string::npos) {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "INTEGER") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "DOUBLE") {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<double>*>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "BOOLEAN") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<bool>*>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "STRING" || inputTypes[i] == "VARCHAR" || inputTypes[i] == "VARCHAR(2147483647)") {
            auto str =
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    vecBatch->Get(pos))
                    ->GetValue(rowId);
            auto strVal = std::make_unique<std::u32string>(str.begin(), str.end());
            row->setString(i, new BinaryStringData(strVal.get()));
        } else {
            LOG("Data type not supported: " << inputTypes[i]);
            delete row;
            throw std::runtime_error("Data type not supported");
        };
    }
    row->setRowKind(vecBatch->getRowKind(rowId));
    return row;
}

void SinkWriterOperator::processBatch(StreamRecord* record)
{
    auto recordGuard = std::unique_ptr<StreamRecord>(record);
    if (record->hasExternalRow()) {
        Row* row_input = reinterpret_cast<Row*>(record->getValue());
        sinkWriter->write(row_input);
        delete row_input;
    } else {
        std::unique_ptr<omnistream::VectorBatch> input(static_cast<omnistream::VectorBatch*>(record->getValue()));
        for (int row = 0; row < input->GetRowCount(); ++row) {
            sinkWriter->write(input.get(), row);
        }
    }
}

void SinkWriterOperator::processElement(StreamRecord* record)
{
    String* input = reinterpret_cast<String*>(record->getValue());
    sinkWriter->write(input);
}

void SinkWriterOperator::EndInput()
{
    endOfInput = true;
    sinkWriter->Flush(true);

    emitCommittables<KafkaCommittable>(std::numeric_limits<std::int64_t>::max());
}

template <typename CommT>
void SinkWriterOperator::emitCommittables(std::int64_t checkpointId)
{
    std::vector<CommT> committables = sinkWriter->prepareCommit();
    int indexOfThisSubtask = 0;
    int numberOfParallelSubtasks = 1;

    emit(indexOfThisSubtask, numberOfParallelSubtasks, checkpointId, committables);
}

template <typename CommT>
void SinkWriterOperator::emit(
    int indexOfThisSubtask,
    int numberOfParallelSubtasks,
    std::int64_t checkpointId,
    const std::vector<CommT>& committables)
{
}

void SinkWriterOperator::ProcessWatermark(Watermark* watermark)
{
}

void SinkWriterOperator::processWatermarkStatus(WatermarkStatus* watermarkStatus)
{
}

void SinkWriterOperator::initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    INFO_RELEASE(
        "savepoint: SinkWriterOperator initializeState with initializer, operatorID: "
        << OneInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<void*>::initializeState(initializer, keySerializer);
    subtaskIndex = initializer->getEnvironment()->taskConfiguration().getIndexOfSubtask();
    this->sinkWriter->SetSubTaskIdx(subtaskIndex);
}

void SinkWriterOperator::notifyCheckpointComplete(long checkpointId)
{
    AbstractStreamOperator<void*>::notifyCheckpointComplete(checkpointId);
}

void SinkWriterOperator::notifyCheckpointAborted(long checkpointId)
{
    AbstractStreamOperator<void*>::notifyCheckpointAborted(checkpointId);
}

// initializeState方法实现
void SinkWriterOperator::initializeState(StateInitializationContextImpl* context)
{
    // 调用父类方法
    AbstractStreamOperator<void*>::initializeState(context);

    // 获取恢复的检查点ID
    std::optional<uint64_t> checkpointId = context->getRestoredCheckpointId();

    // 创建初始化上下文
    auto initContext = createInitContext<void*>(
        checkpointId.has_value() ? std::optional<uint64_t>(static_cast<uint64_t>(*checkpointId))
                                 : std::optional<uint64_t>());

    // 如果是从检查点恢复，处理遗留的提交信息
    if (context->isRestored()) {
        if (committableSerializer != nullptr) {
            // 获取操作符状态存储
            auto* operatorStateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());

            // 获取原始状态
            auto rawState = operatorStateBackend->getListState<std::vector<uint8_t>>(
                &SinkWriterOperator::STREAMING_COMMITTER_RAW_STATES_DESC);

            auto sinkV1 = std::make_shared<SinkV1WriterCommittableSerializer<KafkaCommittable>>(
                std::shared_ptr<SimpleVersionedSerializer<KafkaCommittable>>(
                    committableSerializer, [](SimpleVersionedSerializer<KafkaCommittable>*) {}));

            SimpleVersionedListState<std::vector<KafkaCommittable>> legacyCommitterState(rawState, sinkV1);

            // 处理遗留的提交信息
            auto rawIterable = legacyCommitterState.get();
            if (rawIterable != nullptr) {
                // 将所有元素复制到 legacyCommittables 中
                for (const auto& item : *rawIterable) {
                    for (const auto& committable : item) {
                        legacyCommittables.push_back(committable);
                    }
                }
                delete rawIterable;
            }
        }
    }

    // 创建SinkWriter
    sinkWriter = writerStateHandler->createWriter(initContext, context);
    delete initContext;
}

// snapshotState方法实现
void SinkWriterOperator::snapshotState(StateSnapshotContextSynchronousImpl* context)
{
    // 调用父类方法
    AbstractStreamOperator::snapshotState(context);

    // 快照状态
    writerStateHandler->snapshotState(context->getCheckpointId());
}

std::string SinkWriterOperator::getTypeName()
{
    std::string typeName = "SinkWriterOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}

// 显式实例化模板
template void SinkWriterOperator::emitCommittables<KafkaCommittable>(std::int64_t checkpointId);
template void SinkWriterOperator::emit(
    int indexOfThisSubtask,
    int numberOfParallelSubtasks,
    std::int64_t checkpointId,
    const std::vector<KafkaCommittable>& committables);
