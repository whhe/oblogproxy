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

#include "log.h"

#define SCRAMBLE_LENGTH 20

namespace oceanbase::binlog {
extern bool check_scramble_sha1(const char* token, const char* scramble, const char* hash_stage2);

extern int compute_sha1_hash_multi(const char* buf1, size_t len1, const char* buf2, size_t len2, char* sha1_buf);

extern int compute_sha1_hash(const char* buf, size_t size, char* sha1_buf);

extern int compute_two_stage_sha1_hash(const char* password, size_t size, char* hash_stage1, char* hash_stage2);
};  // namespace oceanbase::binlog
