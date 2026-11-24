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

#pragma once

#include "AbstractStreamingJoinOperator.h"
#include "table/data/JoinedRowData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/data/GenericRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace omnistream {
template <typename K>
class StreamingJoinOperator : public AbstractStreamingJoinOperator<K> {
public:
    StreamingJoinOperator(const nlohmann::json& config, Output* output)
        : AbstractStreamingJoinOperator<K>(config, output)
    {
        this->output = output;
        LOG("<<<<<<JOIN DESC:" << config.dump());
        filterNullKeys = config["filterNulls"].get<std::vector<bool>>();
        if (config["joinType"] == "InnerJoin") {
            leftIsOuter = false;
            rightIsOuter = false;
        } else if (config["joinType"] == "LeftOuterJoin") {
            leftIsOuter = true;
            rightIsOuter = false;
        } else {
            NOT_IMPL_EXCEPTION;
        }
    }

    virtual ~StreamingJoinOperator()
    {
        LOG(" >>> StreamingJoinOperator<K>::~StreamingJoinOperator");
    };

    void open() override;

    void processElement1(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void processElement2(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void processBatch1(StreamRecord* element) override
    {
        LOG("processBatch1(StreamRecord* element)");
        auto input = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());
        processBatch(input, leftRecordStateView_.get(), rightRecordStateView_.get(), true);
        delete element;
    };

    void processBatch2(StreamRecord* element) override
    {
        LOG("processBatch2(StreamRecord* element)");
        auto input = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());
        processBatch(input, rightRecordStateView_.get(), leftRecordStateView_.get(), false);
        delete element;
    };

    void ProcessWatermark1(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(0, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }
    void ProcessWatermark2(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(1, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return this->metrics;
    }

    std::string getTypeName() override
    {
        return this->opName;
    }

protected:
    bool leftIsOuter;
    bool rightIsOuter;
    std::vector<bool> filterNullKeys;
    std::unique_ptr<JoinRecordStateView> leftRecordStateView_ = nullptr;
    std::unique_ptr<JoinRecordStateView> rightRecordStateView_ = nullptr;
    void processElement(
        const std::shared_ptr<RowData>& input,
        JoinRecordStateView* inputSideStateView,
        JoinRecordStateView* otherSideStateView,
        bool inputIsLeft);

    void processBatch(
        omnistream::VectorBatch* input,
        JoinRecordStateView* inputSideStateView,
        JoinRecordStateView* otherSideStateView,
        bool inputIsLeft);

private:
    omnistream::VectorBatch* buildOutputVectorBatch()
    {
        if (outputRows_.empty()) {
            return nullptr;
        }
        const auto outputRowCount = static_cast<int32_t>(outputRows_.size());
        auto* res = new omnistream::VectorBatch(outputRowCount);
        auto isNull = [this](RowData* row, int32_t column) {
            return row == nullptr || row == leftNullRow_.get() || row == rightNullRow_.get() || row->isNullAt(column);
        };
        auto appendColumn = [&](auto* vector, bool fromLeft, int32_t inputColumn, auto valueGetter) {
            for (int32_t outputRow = 0; outputRow < outputRowCount; ++outputRow) {
                auto& joinedRow = outputRows_[outputRow];
                RowData* row = fromLeft ? joinedRow.getRow1() : joinedRow.getRow2();
                if (isNull(row, inputColumn)) {
                    vector->SetNull(outputRow);
                } else {
                    auto value = valueGetter(row, inputColumn);
                    vector->SetValue(outputRow, value);
                }
            }
            res->Append(vector);
        };
        auto appendDecimal128Column = [&](bool fromLeft, int32_t inputColumn) {
            auto* vector = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(
                outputRowCount, omniruntime::type::DataTypeId::OMNI_DECIMAL128);
            for (int32_t outputRow = 0; outputRow < outputRowCount; ++outputRow) {
                auto& joinedRow = outputRows_[outputRow];
                RowData* row = fromLeft ? joinedRow.getRow1() : joinedRow.getRow2();
                if (isNull(row, inputColumn)) {
                    vector->SetNull(outputRow);
                    continue;
                }
                auto* binaryRow = dynamic_cast<BinaryRowData*>(row);
                if (binaryRow == nullptr) {
                    delete vector;
                    throw std::runtime_error("DECIMAL128 join output requires BinaryRowData");
                }
                std::unique_ptr<omniruntime::type::Decimal128> value(binaryRow->getDecimal128(inputColumn, 0));
                if (value == nullptr) {
                    delete vector;
                    throw std::runtime_error("Failed to read DECIMAL128 value from join row");
                }
                vector->SetValue(outputRow, *value);
            }
            res->Append(vector);
        };
        const auto appendInputColumns = [&](const std::vector<int32_t>& types, bool fromLeft) {
            for (int32_t inputColumn = 0; inputColumn < static_cast<int32_t>(types.size()); ++inputColumn) {
                switch (types[inputColumn]) {
                    case omniruntime::type::DataTypeId::OMNI_BYTE:
                        appendColumn(
                            new omniruntime::vec::Vector<int8_t>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) {
                                return *reinterpret_cast<int8_t*>(row->getLong(column));
                            });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_SHORT:
                        appendColumn(
                            new omniruntime::vec::Vector<int16_t>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) {
                                return *reinterpret_cast<int16_t*>(row->getLong(column));
                            });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_INT:
                    case omniruntime::type::DataTypeId::OMNI_DATE32:
                    case omniruntime::type::DataTypeId::OMNI_TIME32:
                    case omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS:
                        appendColumn(
                            new omniruntime::vec::Vector<int32_t>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) { return *row->getInt(column); });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_LONG:
                    case omniruntime::type::DataTypeId::OMNI_DATE64:
                    case omniruntime::type::DataTypeId::OMNI_TIME64:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
                    case omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME:
                    case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                        appendColumn(
                            new omniruntime::vec::Vector<int64_t>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) {
                                return *reinterpret_cast<int64_t*>(row->getLong(column));
                            });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                        appendColumn(
                            new omniruntime::vec::Vector<double>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) {
                                return *reinterpret_cast<double*>(row->getLong(column));
                            });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                        appendColumn(
                            new omniruntime::vec::Vector<bool>(
                                outputRowCount, static_cast<omniruntime::type::DataTypeId>(types[inputColumn])),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) { return *row->getBool(column); });
                        break;
                    case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                        appendDecimal128Column(fromLeft, inputColumn);
                        break;
                    case omniruntime::type::DataTypeId::OMNI_CHAR:
                    case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                    case omniruntime::type::DataTypeId::OMNI_VARBINARY:
                        appendColumn(
                            new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                                outputRowCount),
                            fromLeft,
                            inputColumn,
                            [](RowData* row, int32_t column) { return row->getStringView(column); });
                        break;
                    default:
                        delete res;
                        throw std::runtime_error(
                            "Unsupported join output column type: " + std::to_string(types[inputColumn]));
                }
            }
        };

        appendInputColumns(this->leftInputTypes, true);
        appendInputColumns(this->rightInputTypes, false);
        for (int32_t index = 0; index < outputRowCount; ++index) {
            res->setRowKind(index, outputRows_[index].getRowKind());
            res->setTimestamp(index, outputTimestamps_[index]);
        }
        return res;
    }

    std::unique_ptr<RowData> leftNullRow_;
    std::unique_ptr<RowData> rightNullRow_;
    std::vector<JoinedRowData> outputRows_;
    // JoinedRowData deliberately keeps raw pointers.  Retain the shared owners
    // until buildOutputVectorBatch() has copied every output field.
    std::vector<std::pair<std::shared_ptr<RowData>, std::shared_ptr<RowData>>> outputRowOwners_;
    std::vector<int64_t> outputTimestamps_;
    int64_t currentInputRowTimestamp_ = 0;

    void outputNormally(
        JoinedRowData& joinedRowData,
        const std::shared_ptr<RowData>& inputRow,
        const std::shared_ptr<RowData>& otherRow,
        bool inputIsLeft)
    {
        if (inputIsLeft) {
            joinedRowData.replace(inputRow.get(), otherRow.get());
            outputRowOwners_.emplace_back(inputRow, otherRow);
        } else {
            joinedRowData.replace(otherRow.get(), inputRow.get());
            outputRowOwners_.emplace_back(otherRow, inputRow);
        }
        outputRows_.push_back(joinedRowData);
        outputTimestamps_.push_back(currentInputRowTimestamp_);
    }

    void outputNullPadding(JoinedRowData& joinedRowData, const std::shared_ptr<RowData>& inputRow, bool inputIsLeft)
    {
        if (inputIsLeft) {
            joinedRowData.replace(inputRow.get(), rightNullRow_.get());
            outputRowOwners_.emplace_back(inputRow, nullptr);
        } else {
            joinedRowData.replace(leftNullRow_.get(), inputRow.get());
            outputRowOwners_.emplace_back(nullptr, inputRow);
        }
        outputRows_.push_back(joinedRowData);
        outputTimestamps_.push_back(currentInputRowTimestamp_);
    }
};

extern template class StreamingJoinOperator<RowData*>;
extern template class StreamingJoinOperator<long>;
} // namespace omnistream
