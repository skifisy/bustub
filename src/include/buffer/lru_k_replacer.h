//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <deque>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class QueueType { None = 0, History, Cache };

class LRUKNode {
 public:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  [[maybe_unused]] std::list<size_t> history_;  // 最近k次访问的时间戳
  [[maybe_unused]] size_t k_{0};
  [[maybe_unused]] frame_id_t fid_{-1};
  [[maybe_unused]] bool is_evictable_{false};
  QueueType queue_type_{QueueType::None};
  size_t last_visit_{0};  // 最后一次访问的时间戳
  LRUKNode *prev_{nullptr};
  LRUKNode *next_{nullptr};
};

class LRUKList {
 public:
  LRUKList() {
    head_.next_ = &head_;
    head_.prev_ = &head_;
    head_.fid_ = -1;
  }

  auto Erase(LRUKNode *node) -> void {
    BUSTUB_ASSERT(size_ > 0, "LRUKList is empty");
    --size_;
    auto prev = node->prev_;
    prev->next_ = node->next_;
    prev->next_->prev_ = prev;
    node->prev_ = nullptr;
    node->next_ = nullptr;
  }

  auto PushBack(LRUKNode *node) -> void {
    ++size_;
    LRUKNode *tail = head_.prev_;
    node->next_ = &head_;
    node->prev_ = tail;
    tail->next_ = node;
    head_.prev_ = node;
  }

  auto PushFront(LRUKNode *node) -> void {
    ++size_;
    LRUKNode *first = head_.next_;
    head_.next_ = node;
    first->prev_ = node;
    node->prev_ = &head_;
    node->next_ = first;
  }

  auto PopBack(LRUKNode *node) -> LRUKNode * {
    BUSTUB_ASSERT(size_ > 0, "LRUKList is empty");
    --size_;
    LRUKNode *last = head_.prev_;
    last->prev_->next_ = last->next_;
    head_.prev_ = last->prev_;
    last->next_ = nullptr;
    last->prev_ = nullptr;
    return last;
  }

  auto Size() const -> size_t { return size_; }

  auto Back() -> LRUKNode * { return head_.prev_; }

  auto Head() -> LRUKNode * { return &head_; }

  auto ToString() -> std::string {
    std::string str = "list: ";
    for (LRUKNode *cur = Back(); cur != Head(); cur = cur->prev_) {
      str.append(std::to_string(cur->fid_));
      str.append(", ");
    }
    return str;
  }

 private:
  LRUKNode head_;
  size_t size_{0};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict() -> std::optional<frame_id_t>;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 public:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  LRUKList history_list_;
  LRUKList cache_list_;
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};         // 当前可以被evict的frame
  const size_t replacer_size_;  // frame总容量
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
