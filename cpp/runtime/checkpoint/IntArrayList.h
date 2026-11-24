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

#ifndef INTARRAYLIST_H
#define INTARRAYLIST_H

#include <vector>
#include <stdexcept>
#include <algorithm>
#include <iostream>

namespace omnistream {
class IntArrayList {
public:
    explicit IntArrayList(size_t capacity) : size_(0), capacity_(capacity), array_(new int[capacity])
    {
    }

    IntArrayList(const IntArrayList&) = delete;
    IntArrayList& operator=(const IntArrayList&) = delete;

    // 移动语义支持
    IntArrayList(IntArrayList&& other) noexcept : size_(other.size_), capacity_(other.capacity_), array_(other.array_)
    {
        other.array_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    IntArrayList& operator=(IntArrayList&& other) noexcept
    {
        if (this != &other) {
            delete[] array_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            array_ = other.array_;
            other.array_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }
        return *this;
    }

    ~IntArrayList()
    {
        delete[] array_;
    }

    size_t size() const
    {
        return size_;
    }

    virtual bool add(int number)
    {
        grow(size_ + 1);
        array_[size_++] = number;
        return true;
    }

    virtual int removeLast()
    {
        if (size_ == 0) {
            throw std::out_of_range("Cannot remove from an empty list.");
        }
        return array_[--size_];
    }

    void clear()
    {
        size_ = 0;
    }

    bool isEmpty() const
    {
        return size_ == 0;
    }

    std::vector<int> toArray() const
    {
        return std::vector<int>(array_, array_ + size_);
    }

private:
    size_t size_;
    size_t capacity_;
    int* array_;

    void grow(size_t length)
    {
        if (length > capacity_) {
            size_t newLength = std::max(static_cast<size_t>(2 * capacity_), length);
            int* newArray = new int[newLength];
            std::copy(array_, array_ + size_, newArray);
            delete[] array_;
            array_ = newArray;
            capacity_ = newLength;
        }
    }
};

struct EmptyImpl : public IntArrayList {
    EmptyImpl() : IntArrayList(0)
    {
    }

    bool add(int) override
    {
        throw std::runtime_error("Cannot add to an empty-only IntArrayList.");
    }

    int removeLast() override
    {
        throw std::runtime_error("Cannot remove from an empty-only IntArrayList.");
    }
};

} // namespace omnistream

#endif
