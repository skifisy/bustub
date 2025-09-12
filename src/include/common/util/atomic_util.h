//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// atomic_util.h
//
// Identification: src/include/common/util/atomic_util.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

namespace bustub {
class AtomicUtil {
 public:
  template <typename T>
  static inline void SafeDecrementIfPositive(std::atomic<T> &counter) {
    T current = counter.load(std::memory_order_relaxed);
    while (current > 0) {
      if (counter.compare_exchange_weak(current, current - 1, std::memory_order_release, std::memory_order_relaxed)) {
        break;  // 成功递减
      }
      // 如果失败，current已被更新为最新值，循环继续尝试
    }
    // 如果current == 0，直接退出
  }
};
}  // namespace bustub