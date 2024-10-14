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

#include <vector>
#include <functional>
#include "common.h"
#include "log.h"
#include "config.h"

namespace oceanbase::logproxy {

#define oms_enum(_clazz_) _clazz_::e()

#define oms_enum_dcl(_clazz_)                              \
public:                                                    \
  static _clazz_& e();                                     \
  const std::map<std::string, int>& names() const override \
  {                                                        \
    return _name_refs;                                     \
  }                                                        \
  const std::map<int, std::string>& codes() const override \
  {                                                        \
    return _code_refs;                                     \
  }                                                        \
                                                           \
private:                                                   \
  explicit _clazz_(const std::string& name) : Enum(name)   \
  {}                                                       \
  class Reg {                                              \
  public:                                                  \
    Reg(_clazz_* inst, int c, const std::string& n);       \
  };                                                       \
                                                           \
private:                                                   \
  std::map<std::string, int> _name_refs;                   \
  std::map<int, std::string> _code_refs

#define enum_def(_clazz_)                                       \
  _clazz_::Reg::Reg(_clazz_* inst, int c, const std::string& n) \
  {                                                             \
    inst->_name_refs.emplace(n, c);                             \
    inst->_code_refs.emplace(c, n);                             \
  }                                                             \
  _clazz_& _clazz_::e()                                         \
  {                                                             \
    static _clazz_ _enum_(#_clazz_);                            \
    return _enum_;                                              \
  }

#if __cplusplus >= 201703L
#define enum_item_def(_code_, _name_)         \
public:                                       \
  constexpr static int _name_##_code{_code_}; \
  const EnumItem _name_{_code_, #_name_};     \
                                              \
private:                                      \
  const Reg _reg_##_name_                     \
  {                                           \
    this, _code_, #_name_                     \
  }
#else
#define enum_item_def(_code_, _name_)     \
public:                                   \
  const EnumItem _name_{_code_, #_name_}; \
                                          \
private:                                  \
  const Reg _reg_##_name_                 \
  {                                       \
    this, _code_, #_name_                 \
  }
#endif

#define enum_item_code_def(_code_, _name_)    \
public:                                       \
  constexpr static int _name_##_code{_code_}; \
  const EnumItem _name_{_code_, #_name_};     \
                                              \
private:                                      \
  const Reg _reg_##_name_                     \
  {                                           \
    this, _code_, #_name_                     \
  }

class Enum {
public:
  virtual ~Enum() = default;

  class EnumItem {
    OMS_DEFMOVE_NO_CTOR(EnumItem)

  public:
    EnumItem() = default;

    EnumItem(int c, const std::string& n) : code(c), name(n)
    {}

    bool available() const
    {
      return !name.empty();
    }

    std::string str() const
    {
      return name + "[" + std::to_string(code) + "]";
    }

    friend bool operator==(const EnumItem& lhs, const EnumItem& rhs)
    {
      return lhs.code == rhs.code;
    }

    friend bool operator!=(const EnumItem& lhs, const EnumItem& rhs)
    {
      return !(rhs == lhs);
    }

    friend bool operator<(const EnumItem& lhs, const EnumItem& rhs)
    {
      return lhs.code < rhs.code;
    }

    int code = 0;
    std::string name;
  };

  virtual const std::map<std::string, int>& names() const = 0;

  virtual const std::map<int, std::string>& codes() const = 0;

  EnumItem get_by_name(const std::string& name) const;

  EnumItem get_by_code(int code) const;

  EnumItem get(const std::string& name) const;

  EnumItem get(int code) const;

  bool exist(const std::string& name) const;

  bool exist(int code) const;

protected:
  explicit Enum(const std::string& name) : _name(name)
  {}

protected:
  std::string _name;
};

typedef Enum::EnumItem EnumItem;
}  // namespace oceanbase::logproxy
