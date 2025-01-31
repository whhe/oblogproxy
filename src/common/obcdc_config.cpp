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

#include <vector>
#include <sstream>
#include "log.h"
#include "str.h"
#include "obaccess/ob_access.h"
#include "fs_util.h"
#include "obcdc_config.h"

namespace oceanbase::logproxy {

ObcdcConfig::ObcdcConfig(const std::string& str) : ConfigBase(OBCDC_CONFIG)
{
  // syntax: "k1=v1 k2=v2 k3=v3"
  add_all(str);
}

void ObcdcConfig::add(const std::string& key, const std::string& value)
{
  auto entry = _configs.find(key);
  if (entry == _configs.end()) {
    _extras.emplace(key, value);
  } else {
    entry->second->from_str(value);
  }
}

void ObcdcConfig::add_all(const std::string& str)
{
  std::vector<std::string> kvs;
  split(str, ' ', kvs);
  for (std::string& kv : kvs) {
    std::vector<std::string> kv_split;
    int count = split(kv, '=', kv_split, true);
    if (count != 2) {
      continue;
    }
    add(kv_split[0], kv_split[1]);
  }
}

void ObcdcConfig::set(const std::string& key, const std::string& value)
{
  auto entry = _configs.find(key);
  if (entry == _configs.end()) {
    auto eentry = _extras.find(key);
    if (eentry == _extras.end()) {
      _extras.emplace(key, value);
    } else {
      eentry->second = value;
    }
  } else {
    entry->second->from_str(value);
  }
}

void ObcdcConfig::generate_configs(std::map<std::string, std::string>& configs) const
{
  for (auto& entry : _configs) {
    const std::string& val = entry.second->debug_str();
    if (val.empty()) {
      continue;
    }
    configs.emplace(entry.first, val);
  }
  for (auto& entry : _extras) {
    if (entry.second.empty()) {
      continue;
    }
    configs.emplace(entry.first, entry.second);
  }
}

std::string ObcdcConfig::to_string(bool formatted) const
{
  std::stringstream ss;
  for (auto& entry : _configs) {
    ss << entry.first << ":" << entry.second->debug_str() << ",";
    if (formatted) {
      ss << '\n';
    }
  }

  for (auto& entry : _extras) {
    ss << entry.first << ":" << entry.second << ',';
    if (formatted) {
      ss << '\n';
    }
  }
  return ss.str();
}

std::string ObcdcConfig::get(const std::string& key)
{
  auto entry = _extras.find(key);
  if (entry == _extras.end()) {
    return "";
  } else {
    return entry->second;
  }
}

void ObcdcConfig::to_json(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const
{
  writer.Key(OBCDC_CONFIG);
  writer.StartObject();

  // configs
  writer.Key(CONFIGS);
  writer.StartObject();
  for (auto entry : _configs) {
    entry.second->write_item(writer);
  }
  writer.EndObject();

  // extras
  writer.Key(EXTRAS);
  writer.StartObject();
  for (auto& entry : _extras) {
    writer.Key(entry.first.c_str());
    writer.String(entry.second.c_str());
  }
  writer.EndObject();

  writer.EndObject();
}

int ObcdcConfig::from_json(const rapidjson::Value& json)
{
  if (!json.HasMember(CONFIGS) || !json[CONFIGS].IsObject() || json[CONFIGS].IsNull() || !json.HasMember(EXTRAS) ||
      !json[EXTRAS].IsObject() || json[EXTRAS].IsNull()) {
    OMS_ERROR("Invalid format: {}", OBCDC_CONFIG);
    return OMS_FAILED;
  }

  const rapidjson::Value& configs = json[CONFIGS];
  ConfigBase::from_json(configs);

  const rapidjson::Value& extras = json[EXTRAS];
  for (auto iter = extras.MemberBegin(); iter != extras.MemberEnd(); ++iter) {
    add((iter->name).GetString(), (iter->value).GetString());
  }
  return OMS_OK;
}

int TenantDbTable::from(const std::string& table_whites)
{
  std::vector<std::string> sections;
  split(table_whites, '|', sections);
  for (auto& section : sections) {
    std::vector<std::string> items;
    int count = split(section, '.', items);
    if (count != 3) {
      OMS_STREAM_ERROR << "Failed to parse table_white_list, invalid syntax:" << section;
      return OMS_FAILED;
    }

    const std::string& tenant = items[0];
    if (tenant == "sys") {
      with_sys = true;
    }
    if (tenant == "*") {
      with_sys = true;
      all_tenant = true;
      tenants.clear();
      return OMS_OK;
    }

    auto tenant_entry = tenants.find(tenant);
    if (tenant_entry == tenants.end()) {
      tenants.emplace(tenant, DbTable());
      tenant_entry = tenants.find(tenant);
    } else if (tenant_entry->second.all_database) {
      continue;
    }

    const std::string& database = items[1];
    if (database == "*") {
      tenant_entry->second.all_database = true;
      tenant_entry->second.databases.clear();
      continue;
    }

    const std::string& table = items[2];
    auto table_entry = tenant_entry->second.databases.find(database);
    if (table_entry == tenant_entry->second.databases.end()) {
      tenant_entry->second.databases.emplace(database, std::set<std::string>{table});
    } else if (!table_entry->second.count("*")) {
      table_entry->second.emplace(table);
    }
  }
  return OMS_OK;
}

void config_password(ObcdcConfig& config, bool is_child)
{
  if (is_child) {
    MysqlProtocol::do_sha_password(config.password.val(), config.password_sha1);
  } else {
    hex2bin(config.password.val().c_str(), config.password.val().size(), config.password_sha1);
  }

  if (!config.sys_password.empty()) {
    config.password.set(config.sys_password.val());
    MysqlProtocol::do_sha_password(config.sys_password.val(), config.sys_password_sha1);
  } else {
    config.password.set(Config::instance().ob_sys_password.val());
    MysqlProtocol::do_sha_password(Config::instance().ob_sys_password.val(), config.sys_password_sha1);
  }
}

}  // namespace oceanbase::logproxy
