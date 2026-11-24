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

#include "FsCheckpointStateOutputStream.h"
#include <fstream>
#include <memory>
#include <common.h>
#include "FileStateHandle.h"

FsCheckpointStateOutputStream::FsCheckpointStateOutputStream(
    Path basePath, int fs, int bufferSize, int localStateThreshold, bool allowRelativePaths)
    : basePath_(std::move(basePath)),
      fs_(fs),
      bufferSize_(bufferSize),
      localStateThreshold_(localStateThreshold),
      allowRelativePaths_(allowRelativePaths),
      closed_(false)
{
    tempPath_ = Path(basePath_, relativeStatePath_ + ".tmp").toString(); // relativeStatePath_ is UUID
    finalPath_ = Path(basePath_, relativeStatePath_).toString();
    handle = nullptr;
    // 构造失败抛出异常时，由局部智能指针释放文件流
    auto outStream = std::make_unique<std::ofstream>(tempPath_, std::ios::binary);
    if (!outStream->is_open()) {
        throw std::runtime_error("Failed to open temp file for checkpoint output: " + tempPath_);
    }
    outStream_ = outStream.release();
}

void FsCheckpointStateOutputStream::Write(const void* data, size_t length)
{
    static_cast<std::ofstream*>(outStream_)->write(reinterpret_cast<const char*>(data), length);
    fileSize += length;
}

void FsCheckpointStateOutputStream::Flush()
{
    static_cast<std::ofstream*>(outStream_)->flush();
}

long FsCheckpointStateOutputStream::GetPos()
{
    if (!outStream_) {
        throw std::runtime_error("Stream not open");
    }
    auto* fbuf = static_cast<std::ofstream*>(outStream_)->rdbuf();
    std::streampos pos = fbuf->pubseekoff(0, std::ios::cur, std::ios::out);
    if (pos < 0) {
        throw std::runtime_error("Failed to get current position");
    }
    return static_cast<long>(pos);
}

void FsCheckpointStateOutputStream::Sync()
{
    NOT_IMPL_EXCEPTION;
}

void FsCheckpointStateOutputStream::Close()
{
    if (!outStream_) {
        closed_ = true;
        return;
    }

    std::unique_ptr<std::ofstream> stream(static_cast<std::ofstream*>(outStream_));
    outStream_ = nullptr;
    if (stream->is_open()) {
        stream->close();
        if (stream->fail()) {
            closed_ = true;
            INFO_RELEASE("Exception: Failed to close checkpoint output stream.");
            throw std::runtime_error("Failed to close checkpoint output stream");
        }
    }
    closed_ = true;
}

std::shared_ptr<StreamStateHandle> FsCheckpointStateOutputStream::CloseAndGetHandle(char* dataStream)
{
    if (handle == nullptr) {
        handle = std::make_shared<FileStateHandle>(tempPath_, fileSize);
    }
    Close();
    return handle;
}

bool FsCheckpointStateOutputStream::IsClosed()
{
    return closed_;
}
