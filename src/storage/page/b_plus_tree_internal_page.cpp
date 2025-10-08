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

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/config.h"
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
  // 无效key的作用：存储最左指针指向block的最小key
  BUSTUB_ASSERT(index >= 0, "Index must be positive");
  BUSTUB_ASSERT(index < GetSize(), "Index out of bounds");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0, "Index must be positive");
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsInsertSafe() const -> bool { return !IsFull(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsDeleteSafe() const -> bool { return GetSize() > (GetMaxSize() + 1) / 2; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyValue(const KeyType &key, const ValueType &value,
                                                    KeyComparator &comparator) -> bool {
  if (IsFull()) {
    return false;
  }
  int pos = SearchKeyIndex(key, comparator);
  BUSTUB_ASSERT(pos == 0 || comparator(KeyAt(pos), key) != 0, "error");
  InsertKeyValueByIndex(key, value, pos + 1, comparator);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyValueByIndex(const KeyType &key, const ValueType &value, int pos,
                                                           KeyComparator &comparator) {
  BUSTUB_ASSERT(!IsFull(), "page is full");
  // 移动
  for (int i = GetSize() - 1; i >= pos; --i) {
    key_array_[i + 1] = key_array_[i];
    page_id_array_[i + 1] = page_id_array_[i];
    BUSTUB_ASSERT(comparator(key_array_[i + 1], key) > 0, "assertion false");
  }
  // 插入
  key_array_[pos] = key;
  page_id_array_[pos] = value;
  // 更新大小
  SetSize(GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitInternalPage(BPlusTreeInternalPage &other, const KeyType &key,
                                                       const ValueType &value, KeyComparator &comparator) -> KeyType {
  BUSTUB_ASSERT(!IsLeafPage(), "this is not InternalPage");
  BUSTUB_ASSERT(!other.IsLeafPage(), "other is not InternalPage");
  BUSTUB_ASSERT(IsFull(), "this is not full");
  BUSTUB_ASSERT(other.GetSize() == 0, "other is not empty");
  const int max_size = GetMaxSize();
  const int this_size = (max_size - 1) / 2 + 1;
  const int other_size = max_size / 2 + 1;
  SetSize(this_size);
  other.SetSize(other_size);

  int right = other_size - 1;
  int left = max_size - 1;
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
  // 3.1 如果left前面几个都填到right里面，并且key要比left都大
  // 说明key要向上传
  if (left == this_size - 1 && comparator(key_array_[left], key) <= 0) {
    // 更新other的首个指针
    other.key_array_[0] = key;
    other.page_id_array_[0] = value;
    return key;
  }

  auto ret = key_array_[left];
  other.key_array_[0] = ret;
  other.page_id_array_[0] = page_id_array_[left];
  // 3.2 如果key已经填在right里面了，说明left的最右边的值要上传
  if (left == this_size) {
    BUSTUB_ASSERT(comparator(key_array_[left], key) <= 0, "error");
    return ret;
  }
  --left;
  // 4. 否则this继续寻找key的位置
  for (; left >= 1 && comparator(key_array_[left], key) > 0; --left) {
    key_array_[left + 1] = key_array_[left];
    page_id_array_[left + 1] = page_id_array_[left];
  }
  key_array_[left + 1] = key;
  page_id_array_[left + 1] = value;
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchKeyIndex(const KeyType &key, KeyComparator &comparator) const -> int {
  BUSTUB_ASSERT(GetSize() > 1, "internal_size should bigger than 1");  // 内部节点肯定不为空
  auto comp = [&comparator](const KeyType &k1, const KeyType &k2) { return comparator(k1, k2) < 0; };
  auto ptr = std::upper_bound(key_array_ + 1, key_array_ + GetSize(), key, comp);
  return ptr - key_array_ - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKey(const KeyType &key, KeyComparator &comparator, bool is_force) -> bool {
  // 只有当前数量大于一半，才能删除
  if (GetSize() <= (GetMaxSize() + 1) / 2 && !is_force) {
    return false;
  }
  int pos = SearchKeyIndex(key, comparator);
  // pos不可能等于0，因为向左合并
  BUSTUB_ASSERT(pos > 0, "error");
  BUSTUB_ASSERT(pos < GetSize(), "error");
  for (int i = pos; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  SetSize(GetSize() - 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKeyByIndex(int key_index) -> bool {
  // 当前数量大于一半，可以直接删除，无需调整
  bool ret = GetSize() > (GetMaxSize() + 1) / 2;

  BUSTUB_ASSERT(key_index >= 0, "error");
  BUSTUB_ASSERT(key_index < GetSize(), "error");

  for (int i = key_index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  SetSize(GetSize() - 1);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CombinePage(BPlusTreeInternalPage &other) {
  BUSTUB_ASSERT(GetSize() + other.GetSize() <= GetMaxSize(), "error");
  int left = GetSize();
  int right = 0;
  for (; right < other.GetSize(); right++, left++) {
    key_array_[left] = other.key_array_[right];
    page_id_array_[left] = other.page_id_array_[right];
  }
  BUSTUB_ASSERT(GetSize() + other.GetSize() == left, "error");
  SetSize(left);
  other.SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
