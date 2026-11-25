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

#ifndef FLINK_TNEL_TUPLE_H
#define FLINK_TNEL_TUPLE_H

#include "Object.h"
#include "basictypes/ObjectPool.h"

class Tuple2 : public Object {
public:
    Tuple2(Object* f0, Object* f1);

    Tuple2();

    ~Tuple2();

    inline void SetF0(Object* obj);

    inline Object* GetF0();

    inline void SetF1(Object* obj);

    inline void Set(Object* obj1, Object* obj2);

    inline Object* GetF1();

    inline int hashCode() override;

    inline bool equals(Object* obj) override;

    inline Object* clone() override;

    inline void putRefCount();

    static inline Tuple2* of(Object* f0, Object* f1);

    Tuple2* next = nullptr;

    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(Tuple2)) + (f0 ? f0->sizeInBytes() : 0) + (f1 ? f1->sizeInBytes() : 0);
    }

public:
    Object* f0 = nullptr;
    Object* f1 = nullptr;
};

inline void Tuple2::SetF0(Object* obj)
{
    if (f0 != obj) {
        if (obj != nullptr) {
            obj->getRefCount();
        }
        if (f0 != nullptr) {
            f0->putRefCount();
        }
        f0 = obj;
    }
}

inline Object* Tuple2::GetF0()
{
    return f0;
}

inline void Tuple2::SetF1(Object* obj)
{
    if (f1 != obj) {
        if (obj != nullptr) {
            obj->getRefCount();
        }
        if (f1 != nullptr) {
            f1->putRefCount();
        }
        f1 = obj;
    }
}

inline void Tuple2::Set(Object* obj1, Object* obj2)
{
    if (f0 != obj1) {
        if (obj1 != nullptr) {
            obj1->getRefCount();
        }
        if (f0 != nullptr) {
            f0->putRefCount();
        }
        f0 = obj1;
    }

    if (f1 != obj2) {
        if (obj2 != nullptr) {
            obj2->getRefCount();
        }
        if (f1 != nullptr) {
            f1->putRefCount();
        }
        f1 = obj2;
    }
}

inline Object* Tuple2::GetF1()
{
    return f1;
}

inline int Tuple2::hashCode()
{
    int f0Hash = f0->hashCode();
    int f1Hash = f1->hashCode();
    return (int)(f0Hash * 31 + f1Hash);
}

inline bool Tuple2::equals(Object* obj)
{
    if (obj == nullptr) {
        return false;
    }
    Tuple2* tuple = reinterpret_cast<Tuple2*>(obj);
    return f0->equals(tuple->GetF0()) && f1->equals(tuple->GetF1());
}

inline Object* Tuple2::clone()
{
    auto k0 = f0->clone();
    auto k1 = f1->clone();
    auto cloned = new Tuple2(k0, k1);
    k0->putRefCount();
    k1->putRefCount();
    return cloned;
}

inline void Tuple2::putRefCount()
{
    if (--refCount <= 0) {
        if (this->isPool) {
            // this->refCount = 1;
            ObjectPool<Tuple2>* tupleObjectPool = ObjectPool<Tuple2>::getInstance();
            this->next = tupleObjectPool->head;
            tupleObjectPool->head = this;
        } else {
            delete this;
        }
    }
}

inline Tuple2* Tuple2::of(Object* f0, Object* f1)
{
    ObjectPool<Tuple2>* tupleObjectPool = ObjectPool<Tuple2>::getInstance();
    Tuple2* curTuple = tupleObjectPool->getObject();
    curTuple->Set(f0, f1);
    return curTuple;
}
#endif // FLINK_TNEL_TUPLE_H
