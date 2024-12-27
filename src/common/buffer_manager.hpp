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
#include <iostream>
#include <cstring>
#include <stdexcept>
namespace oceanbase::logproxy {
class BufferManager {
public:
  BufferManager(const size_t initial_capacity, const size_t growth_step)
      : _buffer(nullptr), _capacity(initial_capacity), _offset(0), _growth_step(growth_step)
  {
    _buffer = static_cast<unsigned char*>(malloc(_capacity));
    if (_buffer == nullptr) {
      throw std::runtime_error("Initial memory allocation failed");
    }
  }

  ~BufferManager()
  {
    free(_buffer);
  }

  BufferManager(const BufferManager&) = delete;
  BufferManager& operator=(const BufferManager&) = delete;

  BufferManager(BufferManager&& other) noexcept
      : _buffer(other._buffer), _capacity(other._capacity), _offset(other._offset), _growth_step(other._growth_step)
  {
    other._buffer = nullptr;
    other._capacity = 0;
    other._offset = 0;
  }

  BufferManager& operator=(BufferManager&& other) noexcept
  {
    if (this != &other) {
      free(_buffer);
      _buffer = other._buffer;
      _capacity = other._capacity;
      _offset = other._offset;
      _growth_step = other._growth_step;
      other._buffer = nullptr;
      other._capacity = 0;
      other._offset = 0;
    }
    return *this;
  }

  void write(const unsigned char* data, size_t length)
  {
    ensure_capacity(length);
    memcpy(_buffer + _offset, data, length);
    _offset += length;
  }

  [[nodiscard]] unsigned char* data() const
  {
    return _buffer;
  }

  [[nodiscard]] unsigned char* data_cur() const
  {
    return _buffer + _offset;
  }

  [[nodiscard]] size_t offset() const
  {
    return _offset;
  }

  [[nodiscard]] size_t capacity() const
  {
    return _capacity;
  }

  void publish(size_t len)
  {
    _offset += len;
  }

  void reset_offset()
  {
    _offset = 0;
  }
  // Determine whether the memory meets the needs. If not, expand the memory according to the step size.
  void ensure_capacity(const size_t length)
  {
    if (_offset + length > _capacity) {
      size_t new_capacity = _capacity + _growth_step;
      while (new_capacity < _offset + length) {
        new_capacity += _growth_step;
      }

      auto* new_buffer = static_cast<unsigned char*>(realloc(_buffer, new_capacity));
      if (new_buffer == nullptr) {
        throw std::runtime_error("Memory reallocation failed");
      }
      _buffer = new_buffer;
      _capacity = new_capacity;
    }
  }

private:
  unsigned char* _buffer;
  size_t _capacity;
  size_t _offset;
  size_t _growth_step;
};
}  // namespace oceanbase::logproxy
