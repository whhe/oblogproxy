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

#include <cstring>
#include <string>
#include <vector>
#include <random>
#include <functional>
#include <map>

namespace oceanbase::logproxy {

#define OMS_OK 0
#define OMS_AGAIN 1
#define OMS_FAILED (-1)
#define OMS_TIMEOUT (-2)
#define OMS_CONNECT_FAILED (-3)
#define OMS_INVALID (-4)
#define OMS_IO_ERROR (-5)
#define OMS_BINLOG_SKIP (-6)
#define OMS_CLIENT_CLOSED (-7)

#define OMS_UNUSED(x) (void)(x)

#define OMS_SINGLETON(__clazz__)            \
public:                                     \
  static __clazz__& instance()              \
  {                                         \
    static __clazz__ __clazz__##_singleton; \
    return __clazz__##_singleton;           \
  }                                         \
                                            \
private:                                    \
  __clazz__()                               \
  {}

#define OMS_AVOID_COPY(__clazz__)       \
private:                                \
  __clazz__(const __clazz__&) = delete; \
  __clazz__& operator=(const __clazz__&) = delete;

#define OMS_ATOMIC_CAS(_it_, _old_, _new_) __sync_bool_compare_and_swap(&_it_, _old_, _new_)

#define OMS_ATOMIC_INC(_it_) __sync_add_and_fetch(&_it_, 1)

#define OMS_ATOMIC_DEC(_it_) __sync_add_and_fetch(&_it_, -1)

#define OMS_DEFMOVE(__clazz__)                        \
public:                                               \
  __clazz__() = default;                              \
  __clazz__(const __clazz__& o) = default;            \
  __clazz__(__clazz__&& o) noexcept = default;        \
  __clazz__& operator=(const __clazz__& o) = default; \
  __clazz__& operator=(__clazz__&& o) noexcept = default;

#define OMS_DEFMOVE_NO_CTOR(__clazz__)                \
public:                                               \
  __clazz__(const __clazz__& o) = default;            \
  __clazz__(__clazz__&& o) noexcept = default;        \
  __clazz__& operator=(const __clazz__& o) = default; \
  __clazz__& operator=(__clazz__&& o) noexcept = default;

bool localhostip(std::string& hostname, std::string& ip);

std::string dumphex(const std::string& str);
void dumphex(const char data[], size_t size, std::string& hex);
int hex2bin(const char data[], size_t size, std::string& bin);

/*
 * @params The @begin range starts ,the @end range ends
 * @returns
 * @description range [begin,end]
 * @date 2023/1/31 19:52
 */
uint64_t random_number(uint64_t end, uint64_t begin = 0);

template <class T>
void release_vector(std::vector<T>& vect)
{
  for (typename std::vector<T>::iterator it = vect.begin(); it != vect.end(); it++) {
    if (NULL != *it) {
      delete *it;
      *it = NULL;
    }
  }
  vect.clear();
}

inline std::string system_err(int error_no)
{
  return std::to_string(error_no) + "(" + strerror(error_no) + ")";
}

template <class T>
std::string join_vector_str(
    std::vector<T>& vec, std::function<std::string(T)>& id_generator, const std::string& sep = ",")
{
  std::string join_str;
  for (auto entry : vec) {
    join_str.append(join_str.empty() ? "" : sep);
    join_str.append(id_generator(entry));
  }
  return join_str;
}

template <class T, class R>
std::string join_map_str(
    std::map<T, R>& vec, std::function<std::string(T, R)>& kv_generator, const std::string& sep = ",")
{
  std::string join_str;
  for (auto entry_pair : vec) {
    join_str.append(join_str.empty() ? "" : sep);
    join_str.append(kv_generator(entry_pair.first, entry_pair.second));
  }
  return join_str;
}

}  // namespace oceanbase::logproxy
