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

#include "enum.h"

namespace oceanbase::logproxy {
class ModelTypeEnum : public Enum {
  oms_enum_dcl(ModelTypeEnum);

  enum_item_def(0, no);
  enum_item_code_def(1, CHAR);
  enum_item_code_def(2, INT16);
  enum_item_code_def(3, INT);
  enum_item_code_def(4, INT64);
  enum_item_code_def(5, UINT16);
  enum_item_code_def(6, UINT32);
  enum_item_code_def(7, UINT64);
  enum_item_code_def(8, BOOL);
  enum_item_code_def(9, FLOAT);
  enum_item_code_def(10, DOUBLE);
  enum_item_code_def(11, STR);

  enum_item_code_def(100, OBJECT);
  enum_item_code_def(101, LIST);
};

typedef EnumItem ModelType;
}  // namespace oceanbase::logproxy
