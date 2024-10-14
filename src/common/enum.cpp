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

#include "enum.h"
namespace oceanbase::logproxy {
EnumItem Enum::get_by_name(const std::string& name) const
{
  auto it = names().find(name);
  if (it == names().end()) {
    return EnumItem();
  }
  return EnumItem(it->second, it->first);
}

EnumItem Enum::get_by_code(int code) const
{
  auto it = codes().find(code);
  if (it == codes().end()) {
    return EnumItem();
  }
  return EnumItem(it->first, it->second);
}

EnumItem Enum::get(const std::string& name) const
{
  return std::forward<EnumItem>(get_by_name(name));
}

EnumItem Enum::get(int code) const
{
  return std::forward<EnumItem>(get_by_code(code));
}

bool Enum::exist(const std::string& name) const
{
  return names().find(name) != names().end();
}

bool Enum::exist(int code) const
{
  return codes().find(code) != codes().end();
}
}  // namespace oceanbase::logproxy