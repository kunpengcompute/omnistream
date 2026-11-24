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

#ifndef OMNISTREAM_OBJECTBUFFERRECYCLER_H
#define OMNISTREAM_OBJECTBUFFERRECYCLER_H

#include <memory>
#include <string>

#include "ObjectSegment.h"
#include "BufferRecycler.h"
#include "core/memory/Segment.h"

namespace omnistream {

class ObjectBufferRecycler : public BufferRecycler {
public:
    ~ObjectBufferRecycler() override = default;

    std::string toString() const override
    {
        return "ObjectBufferRecycler";
    };
};

class DummyObjectBufferRecycler : public ObjectBufferRecycler {
public:
    static std::shared_ptr<DummyObjectBufferRecycler> getInstance()
    {
        if (!instance) {
            instance = std::make_shared<DummyObjectBufferRecycler>();
        }
        return instance;
    };
    DummyObjectBufferRecycler() = default;
    ~DummyObjectBufferRecycler() override = default;
    void recycle(Segment* objectBuffer) override {};

    std::string toString() const override
    {
        return "DummyObjectBufferRecycler";
    };

private:
    static std::shared_ptr<DummyObjectBufferRecycler> instance;
};

class DeepCopiedObjectBufferRecycler : public ObjectBufferRecycler {
public:
    static std::shared_ptr<DeepCopiedObjectBufferRecycler> getInstance()
    {
        if (!instance) {
            instance = std::make_shared<DeepCopiedObjectBufferRecycler>();
        }
        return instance;
    };
    DeepCopiedObjectBufferRecycler() = default;
    ~DeepCopiedObjectBufferRecycler() override = default;

    void recycle(Segment* objectBuffer) override
    {
        ObjectSegment* segment = dynamic_cast<ObjectSegment*>(objectBuffer);
        if (segment) {
            segment->ReleaseObjects();
            delete segment;
        }
    }

    std::string toString() const override
    {
        return "DeepCopiedObjectBufferRecycler";
    };

private:
    static std::shared_ptr<DeepCopiedObjectBufferRecycler> instance;
};
} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERRECYCLER_H
