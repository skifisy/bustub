//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator() : is_invalid_(true) {}

  ~IndexIterator();  // NOLINT

  explicit IndexIterator(WritePageGuard guard, int pos, BufferPoolManager *bpm)
      : is_invalid_(false), write_guard_(std::move(guard)), pos_(pos), bpm_(bpm) {
    cur_page_ = write_guard_.AsMut<LeafPage>();
  }

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;  // 后缀递增

  auto operator==(const IndexIterator &itr) const -> bool { return is_invalid_ && itr.is_invalid_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return !is_invalid_ || !itr.is_invalid_; }

 private:
  bool is_invalid_;
  WritePageGuard write_guard_;
  LeafPage *cur_page_ = nullptr;
  int pos_ = -1;
  BufferPoolManager *bpm_ = nullptr;
};

}  // namespace bustub
