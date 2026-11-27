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
#pragma once

#include <iterator>
#include <memory>

#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/typeutils/TypeSerializer.h"
#include "functions/SourceContext.h"
#include "functions/SourceFunction.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "table/runtime/operators/source/csv/CsvInputFormat.h"

namespace omnistream {

template <typename OUT>
class InputFormatSourceFunction : public SourceFunction<OUT>, public AbstractRichFunction {
public:
    InputFormatSourceFunction(omnistream::csv::CsvInputFormat<OUT>* format, InputSplit* inputSplit)
        : isRunning_(true),
          format_(format),
          inputSplit_(inputSplit)
    {
    }

    void run(SourceContext* ctx)
    {
#ifdef TROUBLE_SHOOTING
        // in order to trigger back pressure
        for (int i = 0; i < 1000; i++) {
#endif
            int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();
            if (taskId > 0) {
                return;
            }
            INFO_RELEASE("taskId is: " << taskId);
            format_->open(inputSplit_);
            formatOpen_ = true;

            while (isRunning_ && !format_->reachedEnd()) {
                OUT* nextElement = format_->nextRecord();

                if (nextElement != nullptr) {
                    ctx->collect(nextElement);
                } else {
                    break;
                }
            }

            close();
#ifdef TROUBLE_SHOOTING
        }
#endif
    }

    void cancel()
    {
        isRunning_ = false;
    }

    void close() override
    {
        if (formatOpen_) {
            formatOpen_ = false;
            format_->close();
        }
    }

    omnistream::csv::CsvInputFormat<OUT>* getFormat()
    {
        return format_;
    }

    InputSplit* getInputSplit()
    {
        return inputSplit_;
    }

    bool isRunning()
    {
        return isRunning_;
    }

private:
    std::atomic<bool> isRunning_;
    bool formatOpen_ = false;
    omnistream::csv::CsvInputFormat<OUT>* format_;
    InputSplit* inputSplit_;
    int lineCount;
};

} // namespace omnistream
