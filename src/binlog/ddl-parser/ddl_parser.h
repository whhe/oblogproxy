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
#include "common.h"
#include "log.h"

#ifdef BUILD_OPENSOURCE
#include "convert/convert_tool.h"
#else
#include <jni.h>
#include "config.h"
#include <mutex>
#endif

namespace oceanbase::binlog {
class DdlParser {
public:
  ~DdlParser();

  int init();

  int parse(std::string& origin, std::string& result);

#ifndef BUILD_OPENSOURCE
  std::string jstring_2_string(JNIEnv* env, jstring jstr);

  jstring string_2_jstring(JNIEnv* env, const std::string& str);

  bool print_jni_exception_info();

  void jni_error_handler(jint ret) const;

private:
  JavaVM* _jvm = nullptr; /* denotes a Java VM */
  JNIEnv* _env = nullptr; /* pointer to native method interface */
  jclass _cls = nullptr;
  jmethodID _mid;
  std::mutex _jvm_mutex;
#endif
};
}  // namespace oceanbase::binlog
