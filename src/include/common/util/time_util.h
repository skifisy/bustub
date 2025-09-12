//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// time_util.h
//
// Identification: src/include/common/util/time_util.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <chrono>
#include <cstddef>

namespace bustub {

class TimeUtil {
 public:
  static inline auto GetCurrentTimestamp() -> std::size_t {
    // 获取当前时间（系统时钟）
    auto now = std::chrono::system_clock::now();
    // 转换为秒级时间戳（自 1970-01-01 00:00:00 UTC）
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    return static_cast<std::size_t>(seconds.count());
  }

  static inline auto GetCurrentTimestampMs() -> std::size_t {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return static_cast<std::size_t>(ms.count());
  }
};

}  // namespace bustub