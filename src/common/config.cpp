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
#include <fstream>
#include "log.h"
#include "config.h"
#include "jsonutil.hpp"

namespace oceanbase::logproxy {

int Config::load(const std::string& file)
{
  std::ifstream ifs(file, std::ios::in);
  if (!ifs.good()) {
    OMS_FATAL("Failed to open config file: {},reason:{}", file, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  std::string errmsg;
  Json::Value json;
  if (!str2json(ifs, json, &errmsg)) {
    OMS_FATAL("Failed to parse config file: {}, errmsg: {}", file, errmsg);
    return OMS_FAILED;
  }

  for (const std::string& k : json.getMemberNames()) {
    const auto& centry = _configs.find(k);
    if (_configs.count(k) != 0) {
      centry->second->from_str(json[k].asString());
    }
  }

  OMS_INFO("Success to load config: {} ", to_string(true, true));
  return OMS_OK;
}

void Config::init(std::map<std::string, std::string>& configs)
{
  for (const auto& config : configs) {
    if (_configs.count(config.first) != 0) {
      _configs[config.first]->from_str(config.second);
    }
  }
}

int load_configs(const std::string& config_file, rapidjson::Document& doc)
{
  std::ifstream ifs(config_file, std::ios::in);
  if (!ifs.is_open()) {
    OMS_ERROR("Failed to open config file: {}, error: {}", config_file, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  std::string json((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
  ifs.close();
  OMS_INFO("Successfully loaded config json from file: {}", config_file);

  doc.Parse(json.c_str());
  if (doc.HasParseError() || !doc.IsObject()) {
    OMS_ERROR("Failed to parse config file: {}, error: {}, offset: {}",
        config_file,
        doc.GetParseError(),
        doc.GetErrorOffset());
    return OMS_FAILED;
  }

  return OMS_OK;
}

}  // namespace oceanbase::logproxy
