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

#ifndef OMNIFLINK_STREAMFILTER_H
#define OMNIFLINK_STREAMFILTER_H

#include <streaming/runtime/tasks/ChainingOutput.h>
#include <streaming/runtime/tasks/DataStreamChainingOutput.h>

#include "AbstractUdfStreamOperator.h"
#include "functions/FilterFunction.h"
#include "OneInputStreamOperator.h"
#include "core/udf/UDFLoader.h"
/**
 * F: such as Object
 * K: such as Object*, VectorBatch*
 * */
namespace omnistream::datastream {
template <typename F, typename K>
class StreamFilter : public AbstractUdfStreamOperator<FilterFunction<F>, K>, public OneInputStreamOperator {
public:
    StreamFilter(Output* output, nlohmann::json config, bool isStream = true)
    {
        LOG("-----create StreamFilter-----");
        loadUdf(config);
        this->setOutput(output);
        this->isStream = isStream;
    };

    void loadUdf(nlohmann::json config)
    {
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
        std::cout << "filter udf obj: " << udfObj << std::endl;
        auto* symbol = udfLoader.LoadFilterFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }

        this->userFunction = symbol(udfObjJson).release();
    }

    StreamFilter(Output* output, FilterFunction<F>* filterFunction, bool isStream = false)
    {
        LOG("-----create StreamFilter-----");
        this->setOutput(output);
        this->isStream = isStream;
        this->userFunction = filterFunction;
    };

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        // initializeState takes ownership of keySerializer: stateful operators hand it to
        // AbstractKeyedStateBackend, whose destructor frees it. This operator is stateless and
        // has no backend to hand it to, so it frees it here rather than dropping it. The chain
        // allocates a fresh one per operator, so this is never shared. Null for non-SQL types.
        delete keySerializer;
    }

    void processElement(StreamRecord* record) override
    {
        Object* value = reinterpret_cast<Object*>(record->getValue());
        if (this->userFunction->filter(value)) {
            this->output->collect(record);
        } else {
            value->putRefCount();
        }
    }

    void processBatch(StreamRecord* record) override
    {
        omnistream::VectorBatch* input = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
        auto newBatch = this->userFunction->filterBatch(input);
        if (newBatch.empty()) {
            return;
        }
        auto* newVectorBatch = new omnistream::VectorBatch(newBatch.size());
        int32_t colSize = input->GetVectorCount();
        auto colVecs = input->GetVectors();
        for (int32_t i = 0; i < colSize; i++) {
            applyFilterResult(newBatch, newVectorBatch, colVecs, i);
        }
        delete input;
        record->setValue(newVectorBatch);
        AbstractStreamOperator<K>::output->collect(record);
    }

    void applyFilterResult(
        const std::vector<int>& newBatch,
        VectorBatch* newVectorBatch,
        omniruntime::vec::BaseVector* const* colVecs,
        int32_t i) const
    {
        switch (colVecs[i]->GetTypeId()) {
            case (OMNI_LONG): {
                auto oldVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(colVecs[i]);
                auto vec = new omniruntime::vec::Vector<int64_t>(newBatch.size());
                for (size_t j = 0; j < newBatch.size(); j++) {
                    vec->SetValue(j, oldVec->GetValue(newBatch[j]));
                }
                newVectorBatch->Append(vec);
                break;
            }
            case (OMNI_CHAR):
            case (OMNI_VARCHAR): {
                auto oldVec = reinterpret_cast<
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(colVecs[i]);
                auto vec = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                    newBatch.size());
                for (size_t j = 0; j < newBatch.size(); j++) {
                    auto val = oldVec->GetValue(newBatch[j]);
                    vec->SetValue(j, val);
                }
                newVectorBatch->Append(vec);
                break;
            }
            case (OMNI_BOOLEAN): {
                auto oldVec = reinterpret_cast<omniruntime::vec::Vector<bool>*>(colVecs[i]);
                auto vec = new omniruntime::vec::Vector<bool>(newBatch.size());
                for (size_t j = 0; j < newBatch.size(); j++) {
                    vec->SetValue(j, oldVec->GetValue(newBatch[j]));
                }
                newVectorBatch->Append(vec);
                break;
            }
            default: throw std::runtime_error("Unsupported type: ");
        }
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        AbstractStreamOperator<K>::ProcessWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        AbstractStreamOperator<K>::processWatermarkStatus(watermarkStatus);
    }

    void open() override
    {
    }

    void close() override
    {
        AbstractUdfStreamOperator<FilterFunction<F>, K>::close();
    }

    const char* getName() override
    {
        return "StreamFilter";
    }

    std::string getTypeName() override
    {
        std::string typeName = "StreamFilter";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    bool canBeStreamOperator() override
    {
        return this->isStream;
    }

private:
    UDFLoader udfLoader;
};
} // namespace omnistream::datastream
#endif // OMNIFLINK_STREAMFILTER_H
