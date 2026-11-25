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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <rocksdb/iterator.h>
#include <rocksdb/db.h>

#include "core/utils/ByteView.h"

class RocksIteratorWrapper {
public:
    /**
     * 构造函数：接收一个 rocksdb::Iterator 指针，转移所有权
     * @param iter 必须非空
     */
    explicit RocksIteratorWrapper(std::unique_ptr<rocksdb::Iterator> iter) : iterator(std::move(iter))
    {
        if (!iterator) {
            throw std::invalid_argument("Iterator cannot be null");
        }
    }

    RocksIteratorWrapper(const RocksIteratorWrapper&) = delete;
    RocksIteratorWrapper& operator=(const RocksIteratorWrapper&) = delete;

    RocksIteratorWrapper(RocksIteratorWrapper&&) noexcept = default;
    RocksIteratorWrapper& operator=(RocksIteratorWrapper&&) noexcept = default;

    /**
     * 检查迭代器是否有效
     */
    bool isValid()
    {
        bool valid = iterator->Valid();
        if (!valid) {
            status();
        }
        return valid;
    }

    /**
     * 移动到第一个键
     */
    void seekToFirst()
    {
        iterator->SeekToFirst();
    }

    /**
     * 移动到最后一个键
     */
    void seekToLast()
    {
        iterator->SeekToLast();
    }

    /**
     * 查找目标键（字节数组）
     */
    void seek(const std::vector<uint8_t>& target)
    {
        iterator->Seek(rocksdb::Slice(reinterpret_cast<const char*>(target.data()), target.size()));
    }

    /**
     * 反向查找目标键（字节数组）
     */
    void seekForPrev(const std::vector<uint8_t>& target)
    {
        iterator->SeekForPrev(rocksdb::Slice(reinterpret_cast<const char*>(target.data()), target.size()));
    }

    /**
     * 反向查找目标键
     */
    void seekForPrev(const rocksdb::Slice& target)
    {
        iterator->SeekForPrev(target);
    }

    /**
     * 字符串版本（常用）
     */
    void seek(const std::string& target)
    {
        iterator->Seek(rocksdb::Slice(target));
    }

    void seek(const rocksdb::Slice& target)
    {
        iterator->Seek(target);
    }

    void seekForPrev(const std::string& target)
    {
        iterator->SeekForPrev(rocksdb::Slice(target));
    }

    /**
     * 下一个
     */
    void next()
    {
        iterator->Next();
    }

    /**
     * 上一个
     */
    void prev()
    {
        iterator->Prev();
    }

    /**
     * 检查内部状态，若出错则抛出 FlinkRuntimeException
     */
    void status()
    {
        try {
            auto status = iterator->status();
            if (!status.ok()) {
                throw std::runtime_error("Internal exception found in RocksDB: " + status.ToString());
            }
        } catch (...) {
            throw std::runtime_error("Internal exception found in RocksDB");
        }
    }

    /**
     * 刷新迭代器（RocksDB 支持）
     * @throws FlinkRuntimeException 如果刷新失败
     */
    void refresh()
    {
        try {
            iterator->Refresh();
            status();
        } catch (const rocksdb::Status& s) {
            throw std::runtime_error("Refresh failed: " + s.ToString());
        }
    }

    /**
     * 获取当前键
     * @return 字符串（拷贝）
     */
    std::string key()
    {
        return iterator->key().ToString();
    }

    /**
     * 获取当前键
     * @return 字节视图，仅在下一次移动或关闭 iterator 前有效
     */
    ByteView keyView()
    {
        auto slice = iterator->key();
        return ByteView::fromBuffer(slice.data(), slice.size());
    }

    /**
     * 获取当前键
     * @return Slice
     */
    rocksdb::Slice keySlice()
    {
        return iterator->key();
    }

    /**
     * 获取当前值
     * @return 字符串（拷贝）
     */
    std::string value()
    {
        return iterator->value().ToString();
    }

    /**
     * 获取当前值
     * @return 字节视图，仅在下一次移动或关闭 iterator 前有效
     */
    ByteView valueView()
    {
        auto slice = iterator->value();
        return ByteView::fromBuffer(slice.data(), slice.size());
    }

    /**
     * 获取当前值
     * @return Slice
     */
    rocksdb::Slice valueSlice()
    {
        return iterator->value();
    }

    /**
     * 析构函数：自动关闭（RAII 替代 Closeable）
     */
    ~RocksIteratorWrapper() = default;

    /**
     * 显式关闭（可选，用于提前释放）
     */
    void close()
    {
        iterator.reset();
    }

private:
    std::unique_ptr<rocksdb::Iterator> iterator;
};
