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
#ifdef BUILD_OPENSOURCE
#include "ddl_converter.h"
namespace oceanbase {
namespace logproxy {

int DdlConverter::convert(const std::string& source, std::string& dest)
{
  OMS_INFO("convert ddl source sql:[ {} ]", source);

  std::string err_msg;
  int result = etransfer::tool::ConvertTool::Parse(source, "", true, dest, err_msg);
  if (result == 0) {
    // success
    OMS_INFO("convert ddl success source sql:[ {} ], dest sql:[ {} ]", source, dest);
    return OMS_OK;
  } else {
    // failed
    OMS_WARN("Failed to convert ddl sql:[ {} ], error message: {}", source, err_msg);
    return OMS_FAILED;
  }
}

}  // namespace logproxy
}  // namespace oceanbase
#endif