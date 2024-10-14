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
#include <deque>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "log.h"
#include "str.h"

namespace oceanbase::logproxy {

typedef enum { OK = 0, ILL_TOKEN, ILL_BEGIN, ILL_END, UNMATCH, NONE_FIELD, UNKNOWN } PatternErr;

typedef enum {
  STATE_INIT = 0,
  STATE_NORMAL,
  STATE_MATCH_TOKEN,
  STATE_FETCH,
  STATE_END_FETCH,
} PatternState;

class SqlPattern {
public:
  typedef enum { LITERAL = 0, FIELD, LIST } StubType;

  class Stub {
  public:
    int type() const
    {
      return _type;
    }

    const std::string& value() const
    {
      return _value;
    }

    void set_type(StubType type)
    {
      _type = type;
    }

    void set_value(const std::string& value)
    {
      _value = value;
    }

  private:
    StubType _type;
    std::string _value;
  };

public:
  static bool explode(const std::string& pattern, std::deque<Stub>& pieces, PatternErr& err);

private:
  static const char token_pairs[128];
  static const uint8_t tokens[128];
  static const uint8_t end_tokens[128];
};

class UrlPattern {
public:
  static bool explode_path_vars(const std::string& path, std::map<uint32_t, std::string>& vars, PatternErr& err);

  static bool fetch_path_vars(
      const std::string& path, const std::map<uint32_t, std::string>& vars, std::map<std::string, std::string>& vals);

  static bool fetch_path_vars_prefix(const std::string& path, std::string& prefix);
};

/**
 *
             aa
          /  |  \
        bb  cc   {}
        /    |   | \
       {}   {}  dd  ee
 *
 */
class UrlTrie {
public:
  struct UrlTrieNode {
    std::unordered_map<std::string, UrlTrieNode*> nexts;
    std::pair<std::string, UrlTrieNode*> var;
    bool end = false;
    std::string name;
  };

  virtual ~UrlTrie();

  void clear(UrlTrieNode* node);

  bool put(const std::string& path, const std::string& name);

  bool fetch_vars(const std::string& path, std::string& name, std::map<std::string, std::string>& vals);

private:
  UrlTrieNode _entrys;
};

}  // namespace oceanbase::logproxy
