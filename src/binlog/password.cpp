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

#include "password.h"

#include "obaccess/ob_sha1.h"

using namespace oceanbase::logproxy;
namespace oceanbase::binlog {
bool check_scramble_sha1(const char* token, const char* scramble, const char* hash_stage2)
{
  char sha1_combined[SHA1::SHA1_HASH_SIZE];
  char hash_stage1[SHA1::SHA1_HASH_SIZE];
  char hash_stage2_reassured[SHA1::SHA1_HASH_SIZE];

  /* create key to encrypt scramble */
  if (OMS_OK != compute_sha1_hash_multi(scramble, SCRAMBLE_LENGTH, hash_stage2, SHA1::SHA1_HASH_SIZE, sha1_combined)) {
    OMS_ERROR("Computed sha1 hash for scramble contact hash stage2 failed");
    return false;
  }

  /* encrypt scramble */
  my_xor(reinterpret_cast<const unsigned char*>(sha1_combined),
      reinterpret_cast<const unsigned char*>(token),
      SHA1::SHA1_HASH_SIZE,
      reinterpret_cast<unsigned char*>(hash_stage1));

  /* now buf supposedly contains hash_stage1: so we can get hash_stage2 */
  if (OMS_OK != compute_sha1_hash(hash_stage1, SHA1::SHA1_HASH_SIZE, hash_stage2_reassured)) {
    OMS_ERROR("Computed sha1 hash for hash stage1 failed");
    return false;
  }
  return memcmp(hash_stage2, hash_stage2_reassured, SHA1::SHA1_HASH_SIZE) == 0 ? true : false;
}

int compute_sha1_hash_multi(const char* buf1, size_t len1, const char* buf2, size_t len2, char* sha1_buf)
{
  char scramble_combined[len1 + len2 + 1];
  strcpy(scramble_combined, buf1);
  strcat(scramble_combined, buf2);

  return compute_sha1_hash(scramble_combined, strlen(scramble_combined), sha1_buf);
}

int compute_sha1_hash(const char* buf, size_t size, char* sha1_buf)
{
  SHA1 sha1;
  SHA1::ResultCode result_code = sha1.input(reinterpret_cast<const unsigned char*>(buf), size);
  if (result_code != SHA1::SHA_SUCCESS) {
    OMS_ERROR("Failed to calc sha1(step input) of passwd stage1 to stage2. result code= {}", result_code);
    return OMS_FAILED;
  }

  sha1.get_result(reinterpret_cast<unsigned char*>(sha1_buf));
  return OMS_OK;
}

int compute_two_stage_sha1_hash(const char* password, size_t size, char* hash_stage1, char* hash_stage2)
{
  if (OMS_OK != compute_sha1_hash(password, size, hash_stage1)) {
    return OMS_FAILED;
  }

  if (OMS_OK != compute_sha1_hash(hash_stage1, SHA1::SHA1_HASH_SIZE, hash_stage2)) {
    return OMS_FAILED;
  }
  return OMS_OK;
}

}  // namespace oceanbase::binlog
