//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <initializer_list>
#include <mutex>
#include <optional>
#include <string>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    return std::nullopt;
  }

  frame_id_t evict_id = INVALID_FRAME_ID;
  size_t min_timestamp = -1;
  LRUKNode *node = nullptr;
  // 1. 优先剔除history_list_
  for (node = history_list_.Back(); node != history_list_.Head(); node = node->prev_) {
    BUSTUB_ASSERT(node->fid_ != INVALID_FRAME_ID, "error");
    if (node->is_evictable_) {
      evict_id = node->fid_;
      BUSTUB_ASSERT(node->queue_type_ == QueueType::History, "error");
      history_list_.Erase(node);
      break;
    }
  }

  // 2. 然后剔除cache_list
  if (evict_id == INVALID_FRAME_ID) {
    for (LRUKNode *n = cache_list_.Back(); n != cache_list_.Head(); n = n->prev_) {
      BUSTUB_ASSERT(n->fid_ != INVALID_FRAME_ID, "error");
      if (!n->is_evictable_) {
        continue;
      }
      if (n->last_visit_ < min_timestamp) {
        min_timestamp = n->last_visit_;
        evict_id = n->fid_;
        node = n;
      }
    }
    if (evict_id == INVALID_FRAME_ID) {
      return std::nullopt;
    }
    if (min_timestamp != static_cast<size_t>(-1)) {
      BUSTUB_ASSERT(node->queue_type_ == QueueType::Cache, "error");
      cache_list_.Erase(node);
    }
  }

  node->k_ = 0;
  node->is_evictable_ = false;
  node->last_visit_ = 0;
  --curr_size_;
  node->next_ = nullptr;
  node->prev_ = nullptr;
  node->queue_type_ = QueueType::None;
  return evict_id;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id != INVALID_FRAME_ID, "error");
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // 插入
    LRUKNode node{{}, 1, frame_id, false, QueueType::History, 0, nullptr, nullptr};
    node_store_.emplace(frame_id, std::move(node));
    BUSTUB_ASSERT(k_ > 1, "LRUK k should bigger than 1");
    history_list_.PushFront(&node_store_[frame_id]);

    // ! RecordAccess的时候如果node不存在，插入node，并让cur_size增加
  } else {
    auto &node = it->second;
    if (node.queue_type_ == QueueType::None) {
      // 加入store，但是并没有进入history队列，也是首次访问
      BUSTUB_ASSERT(node.k_ == 0, "first visit, node k should be 0");
      ++node.k_;
      history_list_.PushFront(&node);
      node.queue_type_ = QueueType::History;
      BUSTUB_ASSERT(k_ > 1, "LRUK k should bigger than 1!");
    } else {
      // 更新访问次数
      ++node.k_;
      if (node.k_ == k_) {
        BUSTUB_ASSERT(node.queue_type_ == QueueType::History, "error");
        // 次数等于k时，移动到cache_list
        history_list_.Erase(&node);
        cache_list_.PushFront(&node);
        node.queue_type_ = QueueType::Cache;
      }
      // update: 不再维护cache_list的顺序
      // else if (node.k_ > k_){
      //   // 访问次数大于k_次，移动到最前
      //   cache_list_.Erase(&node);
      //   cache_list_.PushFront(&node);
      // }
    }
  }
  LRUKNode &node = node_store_[frame_id];
  node.last_visit_ = current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  // BUSTUB_ASSERT(it != node_store_.end(), "can't find frame by id");
  // TODO(frameid invalid): frame_id什么时候为invalid？
  BUSTUB_ASSERT(static_cast<unsigned long>(frame_id) < replacer_size_, "frame_id is too big");
  if (it == node_store_.end()) {
    // 如果不存在该node，那么插入到node_store_中
    LRUKNode node;
    node.is_evictable_ = set_evictable;
    node.fid_ = frame_id;
    node_store_.emplace(frame_id, std::move(node));
    return;
  }
  auto &node = it->second;
  if (node.is_evictable_ != set_evictable) {
    if (set_evictable) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
  }
  node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  auto &node = it->second;
  BUSTUB_ASSERT(node.is_evictable_, "frame is not evictable");
  BUSTUB_ASSERT(node.next_ != nullptr, "LRUKNode is not in history/cache list!");
  if (node.queue_type_ == QueueType::Cache) {
    cache_list_.Erase(&node);
  } else if (node.queue_type_ == QueueType::History) {
    history_list_.Erase(&node);
  }
  node_store_.erase(it);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
