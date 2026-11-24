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

#include "basictypes/java_util_Set.h"

Set::Set() = default;

Set::Set(nlohmann::json jsonObj)
{
    std::unordered_set<nlohmann::json> json_set = jsonObj;
    std::unordered_set<nlohmann::json>::iterator it = json_set.begin();
    for (; it != json_set.end(); it++) {
        if ((*it).is_string()) {
            std::string s = *it;
            this->add(s);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in set");
        }
    }
}

Set::Set(std::unordered_set<std::string> set)
{
    std::unordered_set<std::string>::iterator itset = set.begin();
    for (; itset != set.end(); itset++) {
        this->add(*itset);
    }
}

bool Set::addAll(List* li)
{
    std::list<Object*>::iterator it = li->list.begin();
    bool res = true;
    for (; it != li->list.end(); it++) {
        if (set_.insert(*it).second) (*it)->getRefCount();
    }
    return res;
}

bool Set::addAll(Collection* collection)
{
    // todo implement Set::addAll and add libInterfaceRefs
    return false;
}

bool Set::add(Object* obj)
{
    bool b = set_.insert(obj).second;
    if (b) obj->getRefCount();
    return b;
}

bool Set::add(std::string str)
{
    String* string = new String(str);
    bool b = set_.insert(string).second;
    if (!b) string->putRefCount();
    return b;
}

bool Set::remove(Object* obj)
{
    obj->putRefCount();
    return set_.erase(obj) > 0;
}

bool Set::contains(Object* obj) const
{
    return set_.find(obj) != set_.end();
}

bool Set::contains(const std::string& str) const
{
    String* string = new String(str);
    bool b = set_.find(string) != set_.end();
    string->putRefCount();
    return b;
}

size_t Set::size() const
{
    return set_.size();
}

void Set::clear()
{
    set_.clear();
}

Set::SETIterator::SETIterator(Set* set) : set(set)
{
    current_ = set->set_.begin();
    end_ = set->set_.end();
}

Set::SETIterator::~SETIterator()
{
    set->putRefCount();
}

bool Set::SETIterator::hasNext()
{
    return current_ != end_;
}

Object* Set::SETIterator::next()
{
    (*current_)->getRefCount();
    return *(current_++);
}

Set::~Set()
{
    std::unordered_set<Object*, ObjectSetHash, ObjectSetEqual>::iterator it = set_.begin();
    while (it != set_.end()) {
        (*it)->putRefCount();
        it++;
    }
}

java_util_Iterator* Set::iterator()
{
    java_util_Iterator* it = new SETIterator(this);
    this->getRefCount();
    return it;
}

std::vector<Object*> Set::toArray()
{
    return std::vector<Object*>(set_.begin(), set_.end());
}
