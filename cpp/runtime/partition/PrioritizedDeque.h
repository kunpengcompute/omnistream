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

#ifndef PRIORITIZEDDEQUE_H
#define PRIORITIZEDDEQUE_H

#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <sstream>
#include <functional>
#include <algorithm>
#include <stdexcept>
#include <functional>

namespace omnistream {

template <typename T>
class PrioritizedDeque {
public:
    PrioritizedDeque();
    ~PrioritizedDeque();

    void addPriorityElement(std::shared_ptr<T> element);
    void add(std::shared_ptr<T> element);
    void add(std::shared_ptr<T> element, bool priority, bool prioritize);
    void prioritize(std::shared_ptr<T> element);
    std::vector<std::shared_ptr<T>> asUnmodifiableCollection() const;
    std::shared_ptr<T> getAndRemove(std::function<bool(const std::shared_ptr<T>&)> preCondition);
    std::shared_ptr<T> poll();
    std::shared_ptr<T> peek() const;
    int getNumPriorityElements() const;
    bool containsPriorityElement(std::shared_ptr<T> element) const;
    int size() const;
    int getNumUnprioritizedElements() const;
    typename std::vector<std::shared_ptr<T>>::const_iterator iterator() const;
    void clear();
    bool isEmpty() const;
    bool contains(std::shared_ptr<T> element) const;
    bool equals(const PrioritizedDeque<T>& other) const;
    int hashCode() const;
    std::string toString() const;
    std::shared_ptr<T> peekLast() const;

private:
    std::deque<std::shared_ptr<T>> deque;
    int numPriorityElements;
};

template <typename T>
PrioritizedDeque<T>::PrioritizedDeque() : numPriorityElements(0)
{
}

template <typename T>
PrioritizedDeque<T>::~PrioritizedDeque()
{
}

template <typename T>
void PrioritizedDeque<T>::addPriorityElement(std::shared_ptr<T> element)
{
    if (numPriorityElements == 0) {
        deque.push_front(element);
    } else if (static_cast<size_t>(numPriorityElements) == deque.size()) {
        deque.push_back(element);
    } else {
        std::deque<std::shared_ptr<T>> priorPriority;
        for (int index = 0; index < numPriorityElements; index++) {
            priorPriority.push_front(deque.front());
            deque.pop_front();
        }
        deque.push_front(element);
        for (const auto& priorityEvent : priorPriority) {
            deque.push_front(priorityEvent);
        }
    }
    numPriorityElements++;
}

template <typename T>
void PrioritizedDeque<T>::add(std::shared_ptr<T> element)
{
    deque.push_back(element);
}

template <typename T>
void PrioritizedDeque<T>::add(std::shared_ptr<T> element, bool priority, bool _prioritize)
{
    if (!priority) {
        add(element);
    } else {
        if (_prioritize) {
            prioritize(element);
        } else {
            addPriorityElement(element);
        }
    }
}

template <typename T>
void PrioritizedDeque<T>::prioritize(std::shared_ptr<T> element)
{
    auto it = deque.begin();
    for (int i = 0; i < numPriorityElements && it != deque.end(); ++i, ++it) {
        if (*it == element) {
            return;
        }
    }
    if (it != deque.end() && *it == element) {
        numPriorityElements++;
        return;
    }
    it = std::find(deque.begin(), deque.end(), element);
    if (it != deque.end()) {
        deque.erase(it);
    }
    addPriorityElement(element);
}

template <typename T>
std::vector<std::shared_ptr<T>> PrioritizedDeque<T>::asUnmodifiableCollection() const
{
    return std::vector<std::shared_ptr<T>>(deque.begin(), deque.end());
}

template <typename T>
std::shared_ptr<T> PrioritizedDeque<T>::getAndRemove(std::function<bool(const std::shared_ptr<T>&)> preCondition)
{
    auto it = deque.begin();
    for (int i = 0; it != deque.end(); ++i, ++it) {
        if (preCondition(*it)) {
            if (i < numPriorityElements) {
                numPriorityElements--;
            }
            std::shared_ptr<T> result = *it;
            deque.erase(it);
            return result;
        }
    }
    throw std::out_of_range("Element not found");
}

template <typename T>
std::shared_ptr<T> PrioritizedDeque<T>::poll()
{
    if (deque.empty()) {
        return nullptr;
    }
    std::shared_ptr<T> polled = deque.front();
    deque.pop_front();
    if (numPriorityElements > 0) {
        numPriorityElements--;
    }
    return polled;
}

template <typename T>
std::shared_ptr<T> PrioritizedDeque<T>::peek() const
{
    if (deque.empty()) {
        return nullptr;
    }
    return deque.front();
}

template <typename T>
int PrioritizedDeque<T>::getNumPriorityElements() const
{
    return numPriorityElements;
}

template <typename T>
bool PrioritizedDeque<T>::containsPriorityElement(std::shared_ptr<T> element) const
{
    if (numPriorityElements == 0) {
        return false;
    }
    auto it = deque.begin();
    for (int i = 0; i < numPriorityElements && it != deque.end(); ++i, ++it) {
        if (*it == element) {
            return true;
        }
    }
    return false;
}

template <typename T>
int PrioritizedDeque<T>::size() const
{
    return deque.size();
}

template <typename T>
int PrioritizedDeque<T>::getNumUnprioritizedElements() const
{
    return size() - getNumPriorityElements();
}

template <typename T>
typename std::vector<std::shared_ptr<T>>::const_iterator PrioritizedDeque<T>::iterator() const
{
    return deque.begin();
}

template <typename T>
void PrioritizedDeque<T>::clear()
{
    deque.clear();
    numPriorityElements = 0;
}

template <typename T>
bool PrioritizedDeque<T>::isEmpty() const
{
    return deque.empty();
}

template <typename T>
bool PrioritizedDeque<T>::contains(std::shared_ptr<T> element) const
{
    return std::find(deque.begin(), deque.end(), element) != deque.end();
}

template <typename T>
bool PrioritizedDeque<T>::equals(const PrioritizedDeque<T>& other) const
{
    return numPriorityElements == other.numPriorityElements && deque == other.deque;
}

template <typename T>
int PrioritizedDeque<T>::hashCode() const
{
    size_t seed = 0;
    for (const auto& item : deque) {
        seed ^= std::hash<std::shared_ptr<T>>{}(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    seed ^= std::hash<int>{}(numPriorityElements) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return static_cast<int>(seed);
}

template <typename T>
std::string PrioritizedDeque<T>::toString() const
{
    std::stringstream ss;
    ss << "[";
    for (auto it = deque.begin(); it != deque.end(); ++it) {
        if (it != deque.begin()) {
            ss << ", ";
        }
        if (*it) {
            ss << (**it);
        } else {
            ss << "nullptr";
        }
    }
    ss << "]";
    return ss.str();
}

template <typename T>
std::shared_ptr<T> PrioritizedDeque<T>::peekLast() const
{
    if (deque.empty()) {
        return nullptr;
    }
    return deque.back();
}

} // namespace omnistream

#endif // PRIORITIZEDDE
