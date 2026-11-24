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

#ifndef FLINK_TNEL_STREAMOPERATORWRAPPER_H
#define FLINK_TNEL_STREAMOPERATORWRAPPER_H

#include "../../api/operators/StreamOperator.h"

class StreamOperatorWrapper {
public:
    StreamOperatorWrapper(StreamOperator* op, bool isHead) : wrapped_(op), isHead_(isHead)
    {
        previous_ = nullptr;
        next_ = nullptr;
    }
    ~StreamOperatorWrapper()
    {
        delete wrapped_;
        delete next_;
    }
    void setNext(StreamOperatorWrapper* next)
    {
        next_ = next;
    }
    void setPrevious(StreamOperatorWrapper* prev)
    {
        previous_ = prev;
    }
    void setAsHead()
    {
        isHead_ = true;
    }
    StreamOperator* getStreamOperator()
    {
        return wrapped_;
    }
    StreamOperatorWrapper* getPrevious()
    {
        return previous_;
    }
    StreamOperatorWrapper* getNext()
    {
        return next_;
    }

    void notifyCheckpointComplete(long checkpointId)
    {
        if (wrapped_) {
            wrapped_->notifyCheckpointComplete(checkpointId);
        }
    }

private:
    StreamOperator* wrapped_;
    StreamOperatorWrapper* previous_;
    StreamOperatorWrapper* next_;
    bool isHead_;
};

class ReadIterator {
public:
    ReadIterator(StreamOperatorWrapper* first, bool reverse) : reverse(reverse), current(first)
    {
    }

    bool hasNext() const
    {
        return this->current != nullptr && this->current->getStreamOperator() != nullptr;
    }

    StreamOperatorWrapper* next()
    {
        if (hasNext()) {
            StreamOperatorWrapper* next = current;
            current = reverse ? current->getPrevious() : current->getNext();
            return next;
        }
        throw std::invalid_argument("no such element");
    }

private:
    bool reverse;
    StreamOperatorWrapper* current;
};

#endif
