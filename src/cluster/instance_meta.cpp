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

#include "instance_meta.h"
#include "obaccess/ob_access.h"

#include "log.h"
#include "config.h"

namespace oceanbase::binlog {

static std::map<std::string, std::string> kep_mappings = {{"first_start_timestamp", "start_timestamp"},
    {"start_timestamp", "first_start_timestamp"},
    {"rootserver_list", "rootserver_list"},
    {"cluster_url", "cluster_url"},
    {"cluster_user", "cluster_user"},
    {"cluster_password", "cluster_password"}};

InstanceState value_of(uint8_t state)
{
  std::map<uint8_t, InstanceState> _s_instance_state_map = {{0, InstanceState::INIT},
      {1, InstanceState::STARTING},
      {2, InstanceState::RUNNING},
      {3, InstanceState::FAILED},
      {4, InstanceState::STOP},
      {5, InstanceState::DROP},
      {6, InstanceState::OFFLINE},
      {7, InstanceState::PAUSED},
      {8, InstanceState::GRAYSCALE},
      {9, InstanceState::LOGICAL_DROP}};
  auto iter = _s_instance_state_map.find(state);
  if (_s_instance_state_map.end() == iter) {
    return InstanceState::UNDEFINED;
  }
  return _s_instance_state_map[state];
}

std::string instance_state_str(uint8_t state)
{
  static const char* _s_instance_state_names[] = {
      "Init",
      "Starting",
      "Running",
      "Failed",
      "Stop",
      "Drop",
      "Offline",
      "Paused",
      "Grayscale",
      "LogicalDrop",
      "Undefined"  // InstanceState::end
  };
  if (state >= InstanceState::INIT && state <= InstanceState::LOGICAL_DROP) {
    return _s_instance_state_names[state];
  }
  return _s_instance_state_names[InstanceState::UNDEFINED];
}

InstanceMeta::InstanceMeta(const Config& config, const std::string& local_cluster, const std::string& local_tenant)
{
  _config = config;
  set_cluster(local_cluster);
  set_tenant(local_tenant);
  set_version(__OMS_VERSION__);
}

InstanceMeta::InstanceMeta(const Config& config, const std::string& local_instance_name,
    const std::string& local_cluster, const std::string& local_tenant)
{
  _config = config;
  set_instance_name(local_instance_name);
  set_cluster(local_cluster);
  set_tenant(local_tenant);
  set_version(__OMS_VERSION__);
}

int InstanceMeta::config_server_options()
{
  ObAccess ob_access;
  if (0 == binlog_config()->master_server_id()) {
    std::string server_id;
    if (OMS_OK != ob_access.query_server_id(get_obcdc_config(), server_id)) {
      return OMS_FAILED;
    }
    binlog_config()->set_master_server_id(std::atol(server_id.c_str()));
  }
  if (binlog_config()->master_server_uuid().empty()) {
    std::string server_uuid;
    if (OMS_OK != ob_access.query_server_uuid(get_obcdc_config(), tenant(), server_uuid)) {
      return OMS_FAILED;
    }
    binlog_config()->set_master_server_uuid(server_uuid);
  }

  if (config_primary_secondary_options() != OMS_OK) {
    return OMS_FAILED;
  }

  if (0 == binlog_config()->server_id() || binlog_config()->server_uuid().empty()) {
    OMS_ERROR(
        "Invalid server_id [{}] and server_uuid [{}]", binlog_config()->server_id(), binlog_config()->server_uuid());
    return OMS_FAILED;
  }
  OMS_INFO("Binlog instance [{}] server options, master_server_id: {},  master_server_uuid: {}, server_id: {}, "
           "server_uuid: {},cluster_id: {}, tenant_id: {}",
      instance_name(),
      binlog_config()->master_server_id(),
      binlog_config()->master_server_uuid(),
      binlog_config()->server_id(),
      binlog_config()->server_uuid(),
      cluster_id(),
      tenant_id());
  return OMS_OK;
}

int InstanceMeta::config_primary_secondary_options()
{
  ObAccess ob_access;
  if (cluster_id().empty()) {
    std::string cluster_id;
    if (OMS_OK != ob_access.query_cluster_id(get_obcdc_config(), cluster_id)) {
      return OMS_FAILED;
    }
    set_cluster_id(cluster_id);
  }
  if (tenant_id().empty()) {
    std::string tenant_id;
    if (OMS_OK != ob_access.query_tenant_id(get_obcdc_config(), tenant(), tenant_id)) {
      return OMS_FAILED;
    }
    set_tenant_id(tenant_id);
  }
  OMS_INFO(
      "Binlog instance [{}] server options, cluster_id: {}, tenant_id: {}", instance_name(), cluster_id(), tenant_id());
  return OMS_OK;
}

void InstanceMeta::init_meta(
    std::map<std::string, std::string>& instance_configs, std::map<std::string, std::string>& instance_options)
{
  for (const auto& config : instance_configs) {
    instance_options.insert(config);
  }

  auto* binlog_config = new BinlogConfig();
  binlog_config->init(instance_options);
  this->set_binlog_config(binlog_config);

  auto* cdc_config = new CdcConfig();
  init_obcdc_config(instance_options, cdc_config);
  this->set_cdc_config(cdc_config);

  auto* slot_config = new SlotConfig();
  slot_config->init(instance_options);
  this->set_slot_config(slot_config);
}

void InstanceMeta::update_instance_name(const std::string& l_instance_name)
{
  set_instance_name(l_instance_name);
}

void InstanceMeta::update_version()
{
  set_version(__OMS_VERSION__);
}

void InstanceMeta::init_obcdc_config(std::map<std::string, std::string>& instance_options, CdcConfig* cdc_config)
{
  // 1. init ObcdcConfig
  _obcdc_config.add("cluster", cluster());
  _obcdc_config.add("tenant", tenant());

  // get configs from sql options
  for (const auto& option : instance_options) {
    if (kep_mappings.find(option.first) != kep_mappings.end()) {
      _obcdc_config.add(kep_mappings[option.first], option.second);
    }
  }

  // extra config
  std::map<std::string, std::string>::iterator iter;
  if ((iter = instance_options.find("extra_obcdc_cfg")) != instance_options.end()) {
    _obcdc_config.add_all(iter->second);
  }

  // special config
  if (_obcdc_config.user.empty()) {
    _obcdc_config.user.set(_config.ob_sys_username.val());
  }
  if (_obcdc_config.password.empty()) {
    _obcdc_config.password.set(_config.ob_sys_password.val());
  }
  if (_obcdc_config.table_whites.empty()) {
    std::string white_tables =
        _config.table_whitelist.val().empty() ? tenant() + ".*.*" : _config.table_whitelist.val();
    _obcdc_config.table_whites.set(white_tables);
  }

  if (_obcdc_config.get("memory_limit").empty()) {
    _obcdc_config.add("memory_limit", _config.binlog_memory_limit.val());
  }
  if (_obcdc_config.get("working_mode").empty()) {
    _obcdc_config.add("working_mode", _config.binlog_working_mode.val());
  }

  // fix configs required by obcdc
  _obcdc_config.add("enable_output_trans_order_by_sql_operation", "1");
  _obcdc_config.add("sort_trans_participants", "1");
  _obcdc_config.add("enable_output_hidden_primary_key", "0");
  _obcdc_config.add("enable_convert_timestamp_to_unix_timestamp", "1");
  _obcdc_config.add("enable_output_invisible_column", "1");
  // 4.x enables to output row data in the order of column declaration
  _obcdc_config.add("enable_output_by_table_def", "1");
  _obcdc_config.add("enable_output_virtual_generated_column", "1");

  // 2. convert ObcdcConfig to CdcConfig
  std::map<std::string, std::string> configs;
  _obcdc_config.generate_configs(configs);

  std::string extra_obcdc_cfg_str;
  for (const auto& entry : configs) {
    if (kep_mappings.find(entry.first) != kep_mappings.end()) {
      cdc_config->set_plain(kep_mappings[entry.first], entry.second);
    } else {
      extra_obcdc_cfg_str.append(extra_obcdc_cfg_str.empty() ? "" : " ");
      extra_obcdc_cfg_str.append(entry.first).append("=").append(entry.second);
    }
  }
  cdc_config->set_extra_obcdc_cfg(extra_obcdc_cfg_str);
}

ObcdcConfig& InstanceMeta::get_obcdc_config()
{
  convert_to_obcdc_config(_obcdc_config);
  if (_obcdc_config.password_sha1.empty() || _obcdc_config.sys_password_sha1.empty()) {
    config_password(_obcdc_config);
  }
  return _obcdc_config;
}

void InstanceMeta::convert_to_obcdc_config(ObcdcConfig& obcdc_config)
{
  obcdc_config.start_timestamp.set(cdc_config()->start_timestamp());
  obcdc_config.root_servers.set(cdc_config()->rootserver_list());
  obcdc_config.cluster_url.set(cdc_config()->cluster_url());
  obcdc_config.user.set(cdc_config()->cluster_user());
  obcdc_config.password.set(cdc_config()->cluster_password());
  obcdc_config.sys_user.set(cdc_config()->cluster_user());
  obcdc_config.sys_password.set(cdc_config()->cluster_password());
  // obcdc_config.add("memory_limit", cdc_config()->memory_limit());
  obcdc_config.add_all(cdc_config()->extra_obcdc_cfg());
}

InstanceMeta::InstanceMeta(bool initialize)
{
  if (initialize) {
    auto* binlog_config = new BinlogConfig();
    auto* cdc_config = new CdcConfig();
    auto* slot_config = new SlotConfig();
    this->set_binlog_config(binlog_config);
    this->set_cdc_config(cdc_config);
    this->set_slot_config(slot_config);
  }
}

}  // namespace oceanbase::binlog
