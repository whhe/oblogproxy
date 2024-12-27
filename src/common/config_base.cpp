/**
 * Copyright (c) 2024 OceanBase
 * OceanBase Migration Service LogProxy is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <sstream>
#include "config_base.h"
#include "log.h"
#include "ob_aes256.h"

namespace oceanbase::logproxy {

void ConfigBase::add_item(const std::string& key, ConfigItemBase* item)
{
  _configs.emplace(key, item);
}

int ConfigBase::from_json(const rapidjson::Value& json)
{
  for (auto iter = json.MemberBegin(); iter != json.MemberEnd(); ++iter) {
    const std::string key = (iter->name).GetString();
    const auto& entry = _configs.find(key);
    if (_configs.count(key) != 0) {
      rapidjson::Value value;
      rapidjson::Document::AllocatorType allocator;
      value.CopyFrom(iter->value, allocator);
      entry->second->from_json(value);
    }
  }
  return OMS_OK;
}

void ConfigBase::set(const std::string& key, std::string& value)
{
  auto entry = _configs.find(key);
  if (entry != _configs.end()) {
    entry->second->from_str(value);
  }
}

void ConfigBase::to_json(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const
{
  writer.Key(_name.c_str());
  writer.StartObject();
  for (auto entry : _configs) {
    entry.second->write_item(writer);
  }
  writer.EndObject();
}

std::string ConfigBase::to_string(bool formatted) const
{
  std::string str;
  for (auto& entry : _configs) {
    str.append(entry.first).append("=").append(entry.second->debug_str()).append(",");
    str.append(formatted ? "\n" : "");
  }
  return str;
}

std::string ConfigBase::to_string(bool formatted, bool desensitized) const
{
  std::string str;
  std::string pattern = "password";

  for (auto& entry : _configs) {
    bool is_password_field = false;
    if (entry.first.find(pattern) != std::string::npos) {
      is_password_field = true;
    }

    str.append(entry.first).append("=").append(desensitized && is_password_field ? "******" : entry.second->debug_str()).append(",");
    str.append(formatted ? "\n" : "");
  }
  return str;
}

void EncryptedConfigItem::from_str(const std::string& val)
{
  if (val.empty()) {
    return;
  }

  const char* encrypt_key = nullptr;
  if (!_encrypt_key.empty()) {
    encrypt_key = _encrypt_key.c_str();
  }

  std::string bin_val;
  hex2bin(val.data(), val.size(), bin_val);

  char* decrypted = nullptr;
  int decrypted_len = 0;

  AES aes;
  int ret = encrypt_key == nullptr
                ? aes.decrypt(bin_val.data(), bin_val.size(), &decrypted, decrypted_len)
                : aes.decrypt(encrypt_key, bin_val.data(), bin_val.size(), &decrypted, decrypted_len);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to decrypt: {}", val);
    exit(-1);
  }

  _val.assign(decrypted, decrypted_len);
  free(decrypted);
}

}  // namespace oceanbase::logproxy
