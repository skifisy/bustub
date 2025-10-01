//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  next_page_id_ = -1;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0, "index should bigger than 0");
  BUSTUB_ASSERT(index < GetSize(), "index overflow");
  return key_array_[index];
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0, "index should bigger than 0");
  BUSTUB_ASSERT(index < GetSize(), "index overflow");
  return rid_array_[index];
}
INDEX_TEMPLATE_ARGUMENTS

void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0, "index should bigger than 0");
  BUSTUB_ASSERT(index < GetSize(), "index overflow");
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0, "index should bigger than 0");
  BUSTUB_ASSERT(index < GetSize(), "index overflow");
  rid_array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyValue(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  if (IsFull()) {
    return false;
  }
  int pos = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(key_array_[i], key) < 0) {
      ++pos;
    } else {
      break;
    }
  }
  // 移动
  for (int i = GetSize(); i > pos; --i) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
    BUSTUB_ASSERT(comparator(key_array_[i], key) > 0, "assertion false");
  }
  // 插入
  key_array_[pos] = key;
  rid_array_[pos] = value;
  // 更新大小
  SetSize(GetSize() + 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitLeafPage(BPlusTreeLeafPage &other,  //
                                               const KeyType &key, const ValueType &value, KeyComparator &comparator) {
  BUSTUB_ENSURE(IsLeafPage(), "this is not leaf page");
  BUSTUB_ENSURE(other.IsLeafPage(), "other is not leaf page");
  BUSTUB_ASSERT(IsFull(), "this is not full");
  BUSTUB_ASSERT(other.GetSize() == 0, "other is not empty");
  const int max_size = GetMaxSize();
  const int this_size = (max_size + 1) / 2;
  const int other_size = (max_size + 2) / 2;
  SetSize(this_size);
  other.SetSize(other_size);

  int right = other_size - 1;
  int left = max_size - 1;
  // 1. 先将this移动至other
  for (; right >= 0 && comparator(key_array_[left], key) > 0; --right, --left) {
    other.key_array_[right] = key_array_[left];
    other.rid_array_[right] = rid_array_[left];
  }
  // 2. 如果right未放满，说明key找到位置
  if (right >= 0) {
    other.key_array_[right] = key;
    other.rid_array_[right] = value;
    --right;
    for (; right >= 0; --right, --left) {
      other.key_array_[right] = key_array_[left];
      other.rid_array_[right] = rid_array_[left];
    }
  }
  BUSTUB_ASSERT(right == -1, "right is not -1");
  // 3. key找到位置，可以直接返回
  if (comparator(other.key_array_[0], key) <= 0) {
    BUSTUB_ASSERT(left == this_size - 1, "left is not equal to this_size-1");
    return;
  }
  BUSTUB_ASSERT(left == this_size - 2, "left is not equal to this_size-2");
  // 4. 否则this继续寻找key的位置
  for (; left >= 0 && comparator(key_array_[left], key) > 0; --left) {
    key_array_[left + 1] = key_array_[left];
    rid_array_[left + 1] = rid_array_[left];
  }
  key_array_[left + 1] = key;
  rid_array_[left + 1] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, KeyComparator &comparator, bool is_root) -> bool {
  int min_size = (GetMaxSize() + 1) / 2;
  if (!is_root && GetSize() <= min_size) {
    return false;
  }
  // TODO(optimize) 优化为二分查找
  // 1. 查找删除的key
  int pos = -1;
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(key_array_[i], key) == 0) {
      pos = i;
      break;
    }
  }

  // 2. 找不到也返回true
  if (pos == -1) {
    return true;
  }

  // 3. 删除该key
  for (int i = pos; i < GetSize(); i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  SetSize(GetSize() - 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CombinePage(BPlusTreeLeafPage &other) {
  int this_size = GetSize();
  int other_size = other.GetSize();
  BUSTUB_ASSERT(this_size + other_size <= GetMaxSize(), "error");
  for (int left = this_size, right = 0; right < other_size; right++, left++) {
    key_array_[left] = other.key_array_[right];
    rid_array_[left] = other.rid_array_[right];
  }
  next_page_id_ = other.next_page_id_;
  SetSize(this_size + other_size);
  other.SetSize(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
