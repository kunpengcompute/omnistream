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

#ifndef OMNISTREAM_FILEUTILS_H
#define OMNISTREAM_FILEUTILS_H

#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>
#include <random>
#include <mutex>
#include <functional>
#include <fstream>
#include <system_error>
#include <atomic>

namespace fs = std::filesystem;

class FileUtils {
public:
    // 删除文件或目录（递归）
    static void deleteFileOrDirectory(const fs::path& path)
    {
        auto operation = [](const fs::path& p) {
            if (fs::is_directory(p)) {
                deleteDirectoryInternal(p);
            } else {
                fs::remove(p);
            }
        };

        guardIfThreadSafe(operation, path);
    };

    // 删除目录（递归）
    static void deleteDirectory(const fs::path& directory)
    {
        guardIfThreadSafe(deleteDirectoryInternal, directory);
    };

    // 静默删除目录（不报告错误）
    static void deleteDirectoryQuietly(const fs::path& directory)
    {
        try {
            deleteDirectory(directory);
        } catch (...) {
            // 忽略所有异常
        }
    };

    // 清空目录内容
    static void cleanDirectory(const fs::path& directory)
    {
        guardIfThreadSafe(cleanDirectoryInternal, directory);
    };

    // 递归列出目录内容
    static std::vector<fs::path> listDirectory(const fs::path& directory)
    {
        std::vector<fs::path> files;
        for (const auto& entry : fs::directory_iterator(directory)) {
            files.push_back(entry.path());
        }
        return files;
    };

    // 生成随机文件名
    static std::string getRandomFilename(const std::string& prefix)
    {
        const std::string ALPHABET = "0123456789abcdef";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, ALPHABET.size() - 1);

        std::string filename = prefix;
        for (int i = 0; i < 12; i++) {
            filename += ALPHABET[dist(gen)];
        }
        return filename;
    };

    // 读取文件全部内容（UTF-8）
    static std::string readFileUtf8(const fs::path& file)
    {
        std::ifstream in(file, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Cannot open file: " + file.string());
        }

        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        return content;
    };

    // 写入文件内容（UTF-8）
    static void writeFileUtf8(const fs::path& file, const std::string& contents)
    {
        std::ofstream out(file, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Cannot open file: " + file.string());
        }
        out.write(contents.data(), contents.size());
    };

    // 检查文件是否存在
    static bool exists(const fs::path& path)
    {
        return fs::exists(path);
    }

    // 检查是否是JAR文件
    static bool isJarFile(const fs::path& file)
    {
        return file.extension() == ".jar";
    }

    // 获取当前工作目录
    static fs::path getCurrentWorkingDirectory()
    {
        return fs::current_path();
    }

private:
    // 内部实现方法
    static void deleteFileOrDirectoryInternal(const fs::path& path);
    static void deleteDirectoryInternal(const fs::path& directory)
    {
        if (fs::is_directory(directory)) {
            // 先清空目录内容
            cleanDirectoryInternal(directory);
            // 删除空目录
            fs::remove(directory);
        } else if (fs::exists(directory)) {
            throw std::runtime_error(directory.string() + " is not a directory");
        }
    };
    static void cleanDirectoryInternal(const fs::path& directory)
    {
        if (!fs::exists(directory)) {
            throw std::runtime_error("Directory does not exist: " + directory.string());
        }

        if (!fs::is_directory(directory)) {
            throw std::runtime_error("Path is not a directory: " + directory.string());
        }

        for (const auto& entry : fs::directory_iterator(directory)) {
            if (fs::is_directory(entry)) {
                deleteDirectoryInternal(entry.path());
            } else {
                fs::remove(entry.path());
            }
        }
    };

    // 平台特定的安全执行
    template <typename Func>
    static void guardIfThreadSafe(Func&& func, const fs::path& path)
    {
        func(path);
    };

    static std::mutex& getGlobalDeleteLock()
    {
        static std::mutex globalLock;
        return globalLock;
    }
};

// 路径工具类
class PathUtils {
public:
    // 获取绝对路径
    static fs::path absolutizePath(const fs::path& path)
    {
        return fs::absolute(path);
    }

    // 相对化路径
    static fs::path relativizePath(const fs::path& base, const fs::path& path)
    {
        return fs::relative(path, base);
    }
};

#endif // OMNISTREAM_FILEUTILS_H
