//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index > 0, "Index must be positive");
  BUSTUB_ASSERT(index < GetSize(), "Index out of bounds");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0, "Index must be positive");
  BUSTUB_ASSERT(index < GetSize(), "Index out of bounds");
  key_array_[index] = key;
}

/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0, "Index must be non-negative");
  BUSTUB_ASSERT(index < GetSize(), "Index out of bounds");
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0, "Index must be non-negative");
  BUSTUB_ASSERT(index < GetSize(), "Index out of bounds");
  page_id_array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyValue(const KeyType &key, const ValueType &value,
                                                    KeyComparator &comparator) -> bool {
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
    page_id_array_[i] = page_id_array_[i - 1];
    BUSTUB_ASSERT(comparator(key_array_[i], key) > 0, "assertion false");
  }
  // 插入
  key_array_[pos] = key;
  page_id_array_[pos] = value;
  // 更新大小
  SetSize(GetSize() + 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitInternalPage(BPlusTreeInternalPage &other, const KeyType &key,
                                                       const ValueType &value, KeyComparator &comparator) {
  BUSTUB_ENSURE(IsLeafPage(), "this is not leaf page");
  BUSTUB_ENSURE(other.IsLeafPage(), "other is not leaf page");
  BUSTUB_ASSERT(IsFull(), "this is not full");
  BUSTUB_ASSERT(other.GetSize() == 0, "other is not empty");
  const int this_size = GetMinSize();
  const int max_size = GetMaxSize();
  const int other_size = (GetMaxSize() + 1) / 2;
  SetSize(this_size);
  other.SetSize(other_size);

  int right = other_size;
  int left = max_size;
  // 1. 先将this移动至other
  for (; right >= 1 && comparator(key_array_[left], key) > 0; --right, --left) {
    other.key_array_[right] = key_array_[left];
    other.page_id_array_[right] = page_id_array_[left];
  }
  // 2. 如果right未放满，说明key找到位置
  if (right >= 1) {
    other.key_array_[right] = key;
    other.page_id_array_[right] = value;
    --right;
    for (; right >= 1; --right, --left) {
      other.key_array_[right] = key_array_[left];
      other.page_id_array_[right] = page_id_array_[left];
    }
  }
  BUSTUB_ASSERT(right == 0, "right is not zero");
  // 3. key找到位置，可以直接返回
  if (comparator(other.key_array_[0], key) <= 0) {
    BUSTUB_ASSERT(left == this_size, "left is not equal to this_size");
    return;
  }
  // 4. 否则this继续寻找key的位置
  for (; left >= 1 && comparator(key_array_[left], key) > 0; --left) {
    key_array_[left + 1] = key_array_[left];
    page_id_array_[left + 1] = page_id_array_[left];
  }
  key_array_[left] = key;
  page_id_array_[left] = value;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
