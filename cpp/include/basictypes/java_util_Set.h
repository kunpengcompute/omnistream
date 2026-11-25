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

#ifndef JAVA_UNTIL_SET_H
#define JAVA_UNTIL_SET_H
#include <vector>
#include <stdexcept>
#include <unordered_set>
#include "Object.h"
#include "String.h"
#include "java_util_Iterator.h"
#include "java_util_List.h"
#include "java_util_Collection.h"

struct ObjectSetHash {
    size_t operator()(Object* obj) const
    {
        return obj ? obj->hashCode() : 0;
    }
};

struct ObjectSetEqual {
    bool operator()(Object* a, Object* b) const
    {
        if (a == b) return true;
        if (!a || !b) return false;
        return a->equals(b);
    }
};

class List;
class Set : public Collection {
public:
    bool addAll(List* li);

    bool addAll(Collection* collection);

    bool add(Object* obj);

    bool add(std::string str);

    // todo 需要先查找
    bool remove(Object* obj);

    bool contains(Object* obj) const;

    bool contains(const std::string& str) const;

    size_t size() const;

    void clear();

    class SETIterator : public java_util_Iterator {
        using SetIterator = std::unordered_set<Object*, ObjectSetHash, ObjectSetEqual>::iterator;

    public:
        SETIterator(Set* set);

        ~SETIterator();

        bool hasNext();

        Object* next();

    private:
        SetIterator current_;
        SetIterator end_;
        Set* set;
    };

    ~Set();

    java_util_Iterator* iterator();

    // todo returnType is different from java
    std::vector<Object*> toArray();

    Set();

    Set(nlohmann::json jsonObj);

    Set(std::unordered_set<std::string> set);

    std::unordered_set<Object*, ObjectSetHash, ObjectSetEqual> set_;
};
using HashSet = Set;
#endif
