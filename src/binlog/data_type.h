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

#include "meta_info.h"
#include "codec/byte_decoder.h"
#include "str.h"
#include "str_array.h"
#include "msg_buf.h"
#include "binlog/json_parser.h"
#include "ogrsf_frmts.h"

#include <log_record.h>

using namespace oceanbase::logproxy;

namespace oceanbase::binlog {
enum field_types {
  OB_TYPE_DECIMAL,
  OB_TYPE_TINY,
  OB_TYPE_SHORT,
  OB_TYPE_LONG,
  OB_TYPE_FLOAT,
  OB_TYPE_DOUBLE,
  OB_TYPE_NULL,
  OB_TYPE_TIMESTAMP,
  OB_TYPE_LONGLONG,
  OB_TYPE_INT24,
  OB_TYPE_DATE,
  OB_TYPE_TIME,
  OB_TYPE_DATETIME,
  OB_TYPE_YEAR,
  OB_TYPE_NEWDATE,
  OB_TYPE_VARCHAR,
  OB_TYPE_BIT,
  OB_TYPE_TIMESTAMP2 = 17,
  OB_TYPE_DATETIME2 = 18,
  OB_TYPE_TIME2 = 19,
  OB_TYPE_JSON = 245,
  OB_TYPE_NEWDECIMAL = 246,
  OB_TYPE_ENUM = 247,
  OB_TYPE_SET = 248,
  OB_TYPE_TINY_BLOB = 249,
  OB_TYPE_MEDIUM_BLOB = 250,
  OB_TYPE_LONG_BLOB = 251,
  OB_TYPE_BLOB = 252,
  OB_TYPE_VAR_STRING = 253,
  OB_TYPE_STRING = 254,
  OB_TYPE_GEOMETRY = 255,
  OB_TYPES
};

#define MAX_TIME_STR_LEN 10
#define MAX_DATETIME_STR_LEN 19
#define TIME_ZERO_THREE 0x800000L
#define TIME_ZERO_FIVE 0x8000000000L
#define TIME_ZERO_SIX 0x800000000000L

/**
Occupies (decimals/9)*4+decimals%9 (the number of bytes corresponding to the remaining digits) +
(precision/9)*4+precision%9 (the number of bytes corresponding to the remaining digits), where the integer part is from
low to high, and every 9 is Digital XOR sign (0, -1), stored in big-endian, signed integer (4 bytes). The remaining less
than 9 bits, XOR positive and negative (0, -1) and XOR 0x80, stored in big-endian, signed integer (refer to the above
table for occupied bytes). The fractional part is from high to low, every 9 is digital XOR plus or minus (0,-1), stored
in big endian, signed integer (4 bytes). The remaining less than 9 bits, XOR positive and negative (0, -1) and XOR 0x80,
stored in big-endian, signed integer (refer to the above table for occupied bytes).
 */

#define DECODE_BASE_LEN 9
#define DECIMAL_STORE_BASE_LEN 4
#define DIG_MASK 100000000
#define DIG_BASE 1000000000
static const int32_t powers10[DECODE_BASE_LEN + 1] = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
static const int32_t dig2bytes[DECODE_BASE_LEN + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const int64_t frac_max_array[DECODE_BASE_LEN - 1] = {
    900000000, 990000000, 999000000, 999900000, 999990000, 999999000, 999999900, 999999990};

struct IDate {
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int mill_second;
  int precision;
  int sign = 0;
  // Determine how many prefix zeros the precision value after the decimal point has
  int prefix_zero_num = 0;
};

struct IUnixTime {
  uint64_t sec = 0;
  uint64_t us = 0;
  int precision = 0;
  // Determine how many prefix zeros the precision value after the decimal point has
  int prefix_zero_num = 0;
};

/**
 *
 * @param begin
 * @param col_meta
 * @return Bytes of column metadata
 *
 *  Assemble the column meta in the mysql table map event
 *
 */
int set_column_metadata(unsigned char* begin, IColMeta& col_meta, const std::string& table_name);

/**
 * @param col_meta
 * @param data_len
 * @param data
 * @param data_decode
 * @param table_name
 * @param is_diff_partial
 * @param pos
 * @return
 */
size_t get_column_val_bytes(IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode,
    const std::string& table_name, bool is_diff_partial = false);

size_t convert_binlog_geometry(size_t data_len, const char* data, MsgBuf& data_decode);

size_t int_two_complement(unsigned char* val, size_t len, const char* data);

size_t millis_to_seconds(int64_t ms);

size_t seconds_to_millis(int64_t s);

void str_2_hhmmss(std::string str, IDate& date);

IDate str_2_idate(std::string str);

/**
 *
 * @param str 1662034855.000000
 * @return
 */
IUnixTime str_2_unix_time(const std::string& str);

int remainder_bytes(int remainder);

/*!
 * @brief Get each character set, the maximum number of bytes occupied by a character
 * @param charset
 * @return
 */
int charset_encoding_bytes(const std::string& charset);

size_t convert_binlog_tiny_blob(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_medium_blob(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_long_blob(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_blob(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_tiny(const char* data, MsgBuf& data_decode);

size_t convert_binlog_longlong(const char* data, MsgBuf& data_decode);

size_t convert_binlog_int24(const char* data, MsgBuf& data_decode);

size_t convert_binlog_short(const char* data, MsgBuf& data_decode);

size_t convert_binlog_long(const char* data, MsgBuf& data_decode);

size_t convert_binlog_date(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_json(const char* data, MsgBuf& data_decode, bool is_json_diff);

size_t convert_binlog_year(size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_datetime(IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_time(IColMeta& col_meta, const char* data, MsgBuf& data_decode);

size_t convert_binlog_timestamp(IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_set(IColMeta& col_meta, const char* data, MsgBuf& data_decode);

size_t convert_binlog_enum(IColMeta& col_meta, const char* data, MsgBuf& data_decode);

size_t convert_binlog_decimal(IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode);

size_t convert_binlog_var_string(
    IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode, size_t& col_len);

size_t convert_binlog_double(const char* data, MsgBuf& data_decode);

size_t convert_binlog_float(IColMeta& col_meta, const char* data, MsgBuf& data_decode);

size_t convert_binlog_bit(IColMeta& col_meta, size_t data_len, const char* data, MsgBuf& data_decode, size_t& col_len);

int get_number_len(size_t number);

size_t cutout_or_pad_zero(size_t number, int target_length);

size_t fixed_len(size_t len);

void serialize_before(ILogRecord* record, ITableMeta* table_meta, MsgBuf& before_val, size_t& before_pos,
    unsigned char* before_bitmap, int col_count);

void serialize_after(ILogRecord* record, ITableMeta* table_meta, MsgBuf& after_val, size_t& after_pos,
    RowsEventType rows_event_type, unsigned char* after_bitmap, unsigned char*& partial_cols_bitmap, int col_count,
    bool& has_any_json_diff);

size_t col_val_bytes(ILogRecord* record, ITableMeta* table_meta, MsgBuf& before_val, MsgBuf& after_val,
    size_t& before_pos, size_t& after_pos, RowsEventType rows_event_type, unsigned char* before_bitmap,
    unsigned char* after_bitmap, unsigned char*& partial_cols_bitmap, size_t& partial_cols_bytes);

void fill_bitmap(int col_count, size_t col_bytes, unsigned char* bitmap);
}  // namespace oceanbase::binlog
