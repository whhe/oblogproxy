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

#include "ddl_parser.h"

#ifndef BUILD_OPENSOURCE
#include "str.h"
#include "guard.hpp"
#include <dlfcn.h>
#endif

namespace oceanbase::binlog {

DdlParser::~DdlParser()
{
#ifndef BUILD_OPENSOURCE
  if (_jvm != nullptr) {
    _jvm->DestroyJavaVM();
  }
#endif
}

int DdlParser::init()
{
#ifndef BUILD_OPENSOURCE
  JavaVMInitArgs vm_args;
  memset(&vm_args, 0, sizeof(vm_args));
  std::string options_str = logproxy::Config::instance().binlog_ddl_convert_jvm_options.val();
  std::vector<std::string> parts;
  uint64_t size = logproxy::split_by_str(options_str, "|", parts);
  OMS_INFO("options: {}, option size: {}", options_str, size);

  auto* options = new JavaVMOption[parts.size()];
  if (size != 0) {
    for (size_t i = 0; i < parts.size(); ++i) {
      options[i].optionString = parts[i].data();
      OMS_INFO("option[{}]{}", i, options[i].optionString);
    }
    vm_args.options = options;
  }

  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = parts.size();
  vm_args.ignoreUnrecognized = true;
  for (int i = 0; i < vm_args.nOptions; i++) {
    char* optionString = (char*)vm_args.options[i].optionString;
    if (optionString != nullptr) {
      OMS_INFO("{}", optionString);
    }
  }
  OMS_STREAM_INFO << "Initialized the JVM Begin";
  /* load and initialize a Java VM, return a JNI interface
   * pointer in env */
  jint ret = JNI_CreateJavaVM(&_jvm, (void**)&_env, &vm_args);
  delete[] options;
  if (ret < 0) {
    jni_error_handler(ret);
    return OMS_FAILED;
  }

  OMS_STREAM_INFO << "Successfully initialized the JVM";
  jclass local_class = _env->FindClass(logproxy::Config::instance().binlog_ddl_convert_class.val().data());
  if (print_jni_exception_info() != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to call etransfer JNI";
    return OMS_FAILED;
  }
  _cls = reinterpret_cast<jclass>(_env->NewGlobalRef(local_class));
  OMS_STREAM_INFO << "Successfully find the ddl convert class";
  _mid = _env->GetStaticMethodID(_cls,
      logproxy::Config::instance().binlog_ddl_convert_func.val().data(),
      logproxy::Config::instance().binlog_ddl_convert_func_param.val().data());
  if (print_jni_exception_info() != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to call etransfer JNI";
    return OMS_FAILED;
  }
  OMS_STREAM_INFO << "Successfully initialized the ddl convert function";
#endif
  return OMS_OK;
}

int DdlParser::parse(std::string& origin, std::string& result)
{
#ifdef BUILD_OPENSOURCE
  std::string err_msg;
  int ret = etransfer::tool::ConvertTool::Parse(origin, "", true, result, err_msg);
  if (ret != 0) {
    OMS_WARN("Failed to convert ddl sql:[ {} ], error message: {}", origin, err_msg);
    return OMS_FAILED;
  }
  OMS_INFO("convert ddl success, origin: [ {} ], result: [ {} ]", origin, result);
#else
  std::lock_guard<std::mutex> lock(_jvm_mutex);
  if (JNI_OK != this->_jvm->AttachCurrentThread((void**)&_env, nullptr)) {
    OMS_STREAM_ERROR << "Failed to Attach thread";
    return OMS_FAILED;
  }
  jstring sql = string_2_jstring(_env, origin);
  jstring default_schema = string_2_jstring(_env, "");
  jboolean is_case_sensitive = JNI_TRUE;
  OMS_STREAM_INFO << "parser:[" << jstring_2_string(this->_env, sql) << ","
                  << jstring_2_string(this->_env, default_schema) << "]";
  auto ret = _env->CallStaticObjectMethod(_cls, _mid, sql, default_schema, is_case_sensitive);
  result = jstring_2_string(_env, (jstring)ret);
  if (print_jni_exception_info() != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to call etransfer JNI";
    this->_jvm->DetachCurrentThread();
    return OMS_FAILED;
  }
  _env->DeleteLocalRef(sql);
  _env->DeleteLocalRef(default_schema);
  this->_jvm->DetachCurrentThread();
#endif
  // When the converted SQL is empty, the conversion is considered to have failed
  return result.empty() ? OMS_FAILED : OMS_OK;
}

#ifndef BUILD_OPENSOURCE
void DdlParser::jni_error_handler(jint ret) const
{
  switch (ret) {
    case JNI_EVERSION:
      OMS_STREAM_ERROR << "JNI version error";
      break;
    case JNI_ENOMEM:
      OMS_STREAM_ERROR << "Out of memory error";
      break;
    case JNI_EDETACHED:
      OMS_STREAM_ERROR << "Thread detached from the JVM error";
      break;
    case JNI_EEXIST:
      OMS_STREAM_ERROR << "VM already created error";
      break;
    case JNI_EINVAL:
      OMS_STREAM_ERROR << "Invalid argument error";
      break;
    default:
      OMS_STREAM_ERROR << "Unknown error";
      break;
  }
}

bool DdlParser::print_jni_exception_info()
{
  // Check if any exception is thrown
  if (_env->ExceptionCheck()) {
    // Get exception information
    jthrowable exception = _env->ExceptionOccurred();
    // Output exception information to a string
    auto message = (jstring)_env->CallObjectMethod(
        exception, _env->GetMethodID(_env->FindClass("java/lang/Throwable"), "toString", "()Ljava/lang/String;"));
    const char* msg_chars = _env->GetStringUTFChars(message, 0);
    OMS_STREAM_ERROR << "[etransfer] Exception occurred: " << msg_chars;
    // release string resource
    _env->ReleaseStringUTFChars(message, msg_chars);
    _env->ExceptionClear();
    return OMS_FAILED;
  }
  return OMS_OK;
}

std::string DdlParser::jstring_2_string(JNIEnv* env, jstring jstr)
{
  if (!jstr) {
    return "";
  }
  const char* cstr = env->GetStringUTFChars(jstr, nullptr);
  std::string str(cstr);
  env->ReleaseStringUTFChars(jstr, cstr);
  return str;
}

jstring DdlParser::string_2_jstring(JNIEnv* env, const std::string& str)
{
  return env->NewStringUTF(str.c_str());
}
#endif

}  // namespace oceanbase::binlog
