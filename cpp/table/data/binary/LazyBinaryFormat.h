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

#ifndef OMNIFLINK_LAZYBINARYFORMAT_H
#define OMNIFLINK_LAZYBINARYFORMAT_H

#include "BinarySection.h"
#include "../../../core/typeutils/TypeSerializer.h"

template <typename T>
class LazyBinaryFormat {
public:
    virtual ~LazyBinaryFormat()
    {
        delete object;
        delete binarySection;
    }

    LazyBinaryFormat() {};
    explicit LazyBinaryFormat(T* object) : object(object)
    {
        binarySection = new BinarySection();
    };

    // todo why we need to new A T here?
    LazyBinaryFormat(uint8_t* bytes, int offset, int sizeInBytes) : materialized(true)
    {
        binarySection = new BinarySection(bytes, offset, sizeInBytes);
        object = new T();
    };

    // Getters
    T* getObject()
    {
        return object;
    };
    BinarySection* getBinarySection()
    {
        return binarySection;
    };

    int getOffset()
    {
        if (materialized == false) {
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->offset_;
    };
    int getSizeInBytes()
    {
        if (materialized == false) {
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->sizeInBytes_;
    };

    // Materialization
    void ensureMaterialized();

    uint8_t* getSegment()
    {
        if (materialized == false) {
            THROW_LOGIC_EXCEPTION("Lazy Binary Format was not materialized");
        }
        return binarySection->getSegment();
    };

protected:
    T* object;
    BinarySection* binarySection;
    bool materialized = false;

    // Materializer
    virtual BinarySection* materialize(TypeSerializer* serializer) = 0;
};

#endif // OMNIFLINK_LAZYBINARYFORMAT_H
