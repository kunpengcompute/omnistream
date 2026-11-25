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

#include "basictypes/java_util_List.h"

Object* List::get(int32_t idx)
{
    auto it = list.begin();
    for (int32_t i = 0; i < idx; ++i) {
        ++it;
    }
    return *it;
}

List::List() = default;

void List::clear()
{
    for (auto& element : list) {
        element->putRefCount();
    }
    list.clear();
}

List::List(nlohmann::json jsonObj)
{
    std::list<nlohmann::json> json_list = jsonObj;
    std::list<nlohmann::json>::iterator it = json_list.begin();
    for (; it != json_list.end(); it++) {
        if ((*it).is_string()) {
            std::string s = *it;
            this->add(s);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in list");
        }
    }
}

List::List(std::list<std::string> list)
{
    std::list<std::string>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        this->add(*it);
    }
}

void List::add(Object* a)
{
    if (!a) {
        throw std::runtime_error("add nullptr in the list");
    }
    a->getRefCount();
    list.push_back(a);
}

void List::add(std::string& str)
{
    String* string = new String(str);
    list.push_back((Object*)string);
}

void List::addLast(Object* a)
{
    a->getRefCount();
    list.push_back(a);
}

void List::addFirst(Object* a)
{
    a->getRefCount();
    list.push_front(a);
}

void List::remove(int32_t idx)
{
    Object* obj = this->get(idx);

    this->list.remove(obj);

    obj->putRefCount();
}

void List::remove(Object* ele)
{
    this->list.remove(ele);
    ele->putRefCount();
}

bool List::contains(std::string& str)
{
    std::list<Object*>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        String* node = (String*)(*it);
        if (node->equals(str)) return true;
    }
    return false;
}

bool List::contains(Object* a)
{
    std::list<Object*>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        if ((*it)->equals(a)) return true;
    }
    return false;
}

void List::forEach(java_util_function_Consumer* consumer)
{
    std::list<Object*>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        consumer->accept(*it);
    }
    return;
}

int List::size()
{
    return list.size();
}

bool List::isEmpty()
{
    return list.empty();
}

Object* List::getFirst()
{
    return list.front();
}

Object* List::getLast()
{
    return list.back();
}

Object* List::clone()
{
    List* new_list = new List();

    for (auto& i : list) {
        new_list->add(i);
    }

    return new_list;
}

List::ListIterator::ListIterator(List* innerList, listIterator begin, listIterator end) : innerList(innerList)
{
    current_ = begin;
    lastRet = current_;
}

bool List::ListIterator::hasNext()
{
    return current_ != innerList->list.end();
}

Object* List::ListIterator::next()
{
    (*current_)->getRefCount();
    lastRet = current_;
    return *(current_++);
}

void List::ListIterator::remove()
{
    innerList->remove(*lastRet);
}

List::~List()
{
    auto it = list.begin();
    while (it != list.end()) {
        (*it)->putRefCount();
        ++it;
    }
}

java_util_Iterator* List::iterator()
{
    auto* s_iterator = new ListIterator(this, list.begin(), list.end());
    this->getRefCount();
    return s_iterator;
}
