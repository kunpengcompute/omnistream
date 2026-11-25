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

#ifndef OMNISTREAM_RELATIVEFILESTATEHANDLE_H
#define OMNISTREAM_RELATIVEFILESTATEHANDLE_H

class RelativeFileStateHandle : public FileStateHandle {
public:
    RelativeFileStateHandle(const Path& filePath, const std::string& relativePath, long stateSize)
        : FileStateHandle(filePath, stateSize),
          relativePath_(relativePath)
    {
    }

    RelativeFileStateHandle(const nlohmann::json& json)
        : FileStateHandle(
              json.contains("filePath")
                  ? Path(json["filePath"].get<std::string>())
                  : throw std::invalid_argument("RelativeFileStateHandle 'filePath' field missing"),
              json.contains("stateSize")
                  ? json["stateSize"].get<long>()
                  : throw std::invalid_argument("RelativeFileStateHandle 'stateSize' field missing")),
          relativePath_(
              json.contains("relativePath")
                  ? json["relativePath"].get<std::string>()
                  : throw std::invalid_argument("RelativeFileStateHandle 'relativePath' field missing"))
    {
    }

    const std::string& GetRelativePath() const
    {
        return relativePath_;
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["stateHandleName"] = "RelativeFileStateHandle";
        json["filePath"] = GetFilePath().toString();
        json["relativePath"] = relativePath_;
        json["streamStateHandleID"] = nlohmann::json::parse(GetStreamStateHandleID().ToString());
        json["stateSize"] = GetStateSize();
        return json.dump();
    }

    bool operator==(const RelativeFileStateHandle& o) const
    {
        return FileStateHandle::operator==(o) && relativePath_ == o.relativePath_;
    }

private:
    std::string relativePath_;
};

#endif // OMNISTREAM_RELATIVEFILESTATEHANDLE_H
