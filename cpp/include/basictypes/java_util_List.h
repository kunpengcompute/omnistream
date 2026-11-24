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

// java_util_List
#ifndef JAVA_UNTIL_LIST_H
#define JAVA_UNTIL_LIST_H

#include <string>
#include <list>
#include "java_util_Iterator.h"
#include "Object.h"
#include "String.h"
#include "nlohmann/json.hpp"
#include "java_util_Collection.h"
#include "Integer.h"
#include "Double.h"
#include "java_util_function_Consumer.h"

// todo 不同list子类需要分离

class List : public Collection {
public:
    std::list<Object*> list;

    List();

    List(int count) {};

    List(nlohmann::json jsonObj);

    List(std::list<std::string> list);

public:
    Object* get(int32_t idx);

    void clear();

    void add(Object* a);

    void add(std::string& str);

    void addLast(Object* a);

    void addFirst(Object* a);

    void remove(int32_t idx);

    void remove(Object* ele);

    bool contains(std::string& str);

    bool contains(Object* a);

    int size();

    void forEach(java_util_function_Consumer* consumer);

    //    void clear() {
    //    }
    bool isEmpty();

    Object* getFirst();

    Object* getLast();

    Object* clone() override;

    class ListIterator : public java_util_Iterator {
    public:
        using listIterator = std::list<Object*>::iterator;

        listIterator current_;
        listIterator lastRet;
        List* innerList;

    public:
        ListIterator(List* innerList, listIterator begin, listIterator end);

        ~ListIterator() override
        {
            innerList->putRefCount();
        }

        bool hasNext();

        Object* next();

        void remove() override;
    };

    ~List() override;

    // need free
    java_util_Iterator* iterator();
};

using ArrayList = List;
// add remove contains del  isEmpty size Iterator<E> iterator()
#endif // JAVA_UNTIL_LIST_H
