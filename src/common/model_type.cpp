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

#include "model_type.h"
namespace oceanbase {
namespace logproxy {
enum_def(ModelTypeEnum);

constexpr int ModelTypeEnum::CHAR_code;
constexpr int ModelTypeEnum::INT16_code;
constexpr int ModelTypeEnum::INT_code;
constexpr int ModelTypeEnum::INT64_code;
constexpr int ModelTypeEnum::UINT16_code;
constexpr int ModelTypeEnum::UINT32_code;
constexpr int ModelTypeEnum::UINT64_code;
constexpr int ModelTypeEnum::BOOL_code;
constexpr int ModelTypeEnum::FLOAT_code;
constexpr int ModelTypeEnum::DOUBLE_code;
constexpr int ModelTypeEnum::STR_code;
constexpr int ModelTypeEnum::OBJECT_code;
constexpr int ModelTypeEnum::LIST_code;
}  // namespace logproxy
}  // namespace oceanbase