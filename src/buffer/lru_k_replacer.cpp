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

  auto evict_from_list = [this](LRUKList &list) -> std::optional<frame_id_t> {
    for (LRUKNode *node = list.Back(); node != list.Head(); node = node->prev_) {
      if (node->is_evictable_) {
        frame_id_t fid = node->fid_;
        list.Erase(node);        // 从队列中删除
        node_store_.erase(fid);  // 从memory中删除
        --curr_size_;
        return fid;
      }
    }
    return std::nullopt;
  };

  // 1. 优先剔除history_list_
  if (auto fid = evict_from_list(history_list_)) {
    return fid;
  }
  // 2. 然后剔除cache_list
  if (auto fid = evict_from_list(cache_list_)) {
    return fid;
  }

  return std::nullopt;  // 走不到这里
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // frame > replacer_size_，抛出异常
  // 可以使用BUSTUB_ASSERT
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // 插入
    LRUKNode node{{}, 1, frame_id, false};
    node_store_.emplace(frame_id, std::move(node));
    BUSTUB_ASSERT(k_ > 1, "LRUK k should bigger than 1");
    history_list_.PushFront(&node_store_[frame_id]);
    // ! record的时候如果node不存在，插入node，并让cur_size增加
  } else if (it->second.prev_ == nullptr) {
    // 加入store，但是并没有进入history队列，也是首次访问
    auto &node = it->second;
    ++node.k_;
    history_list_.PushFront(&node);
    BUSTUB_ASSERT(k_ > 1, "LRUK k should bigger than 1!");
  } else {
    // 更新访问次数
    auto &node = it->second;
    ++node.k_;
    if (node.k_ == k_) {
      // 次数等于k时，移动到cache_list
      history_list_.Erase(&node);
      cache_list_.PushFront(&node);
    } else if (node.k_ < k_) {
      // history_list使用FIFO策略
    } else {
      // 访问次数大于k_次，移动到最前
      cache_list_.Erase(&node);
      cache_list_.PushFront(&node);
    }
  }
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
  if (node.k_ >= k_) {
    cache_list_.Erase(&node);
  } else {
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
