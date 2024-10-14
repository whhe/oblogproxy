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

#pragma once

#include <string>

template <class T>
class Caster {
public:
  char cast(const std::string& val, const char& ret)
  {
    return (char)(val.empty() ? '\0' : val[0]);
  }

  int16_t cast(const std::string& val, const int16_t& ret)
  {
    return (int16_t)strtol(val.c_str(), nullptr, 10);
  }

  int cast(const std::string& val, const int& ret)
  {
    return (int)strtol(val.c_str(), nullptr, 10);
  }

  int64_t cast(const std::string& val, const int64_t& ret)
  {
    return strtoll(val.c_str(), nullptr, 10);
  }

  uint16_t cast(const std::string& val, const uint16_t& ret)
  {
    return (uint16_t)strtoul(val.c_str(), nullptr, 10);
  }

  uint32_t cast(const std::string& val, const uint32_t& ret)
  {
    return (uint32_t)strtoul(val.c_str(), nullptr, 10);
  }

  int64_t cast(const std::string& val, const uint64_t& ret)
  {
    return (int64_t)(strtoull(val.c_str(), nullptr, 10));
  }

  bool cast(const std::string& val, const bool& ret)
  {
    for (char c : val) {
      if (c != '0') {
        return true;
      }
    }
    return false;
  }

  float cast(const std::string& val, const float& ret)
  {
    return strtof(val.c_str(), nullptr);
  }

  double cast(const std::string& val, const double& ret)
  {
    return strtod(val.c_str(), nullptr);
  }

  std::string cast(const std::string& val, const std::string& ret)
  {
    return val;
  }
};

template <class T>
T cast(const std::string& val)
{
  return Caster<T>().cast(val, T());
}
