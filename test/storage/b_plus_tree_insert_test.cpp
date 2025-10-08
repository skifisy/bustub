//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, LeafInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  using LeafType = BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
  GenericKey<8> index_key;
  RID rid;
  char buffer[BUSTUB_PAGE_SIZE];
  auto leaf = reinterpret_cast<LeafType *>(buffer);
  leaf->Init(20);
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    // slot_num 为低32位，page_id 为高32位 = 0
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    leaf->InsertKeyValue(index_key, rid, comparator);
  }
  index_key.SetFromInteger(3);
  int pos = leaf->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 2);
  index_key.SetFromInteger(-1);
  pos = leaf->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 0);
  index_key.SetFromInteger(50);
  pos = leaf->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 5);
}

TEST(BPlusTreeTests, InternalInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  using InternalType = BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
  GenericKey<8> index_key;
  char buffer[BUSTUB_PAGE_SIZE];
  auto internal = reinterpret_cast<InternalType *>(buffer);
  internal->Init(20);
  internal->SetSize(2);
  index_key.SetFromInteger(0);
  internal->SetKeyAt(0, index_key);
  index_key.SetFromInteger(1);
  internal->SetKeyAt(1, index_key);
  std::vector<int64_t> keys = {2, 3, 4, 5};
  for (auto key : keys) {
    index_key.SetFromInteger(key);
    internal->InsertKeyValue(index_key, key, comparator);
  }
  std::cout << internal->ToString() << std::endl;
  index_key.SetFromInteger(3);
  int pos = internal->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 3);
  index_key.SetFromInteger(-1);
  pos = internal->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 0);
  index_key.SetFromInteger(1);
  pos = internal->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 1);
  index_key.SetFromInteger(50);
  pos = internal->SearchKeyIndex(index_key, comparator);
  ASSERT_EQ(pos, 5);
}

TEST(BPlusTreeTests, BasicInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid);

  auto root_page_id = tree.GetRootPageId();
  auto root_page_guard = bpm->ReadPage(root_page_id);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = root_page_guard.As<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>>();
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  delete bpm;
}

TEST(BPlusTreeTests, InsertTest1NoIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  // 叶子节点2， 中间节点3
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    // slot_num 为低32位，page_id 为高32位 = 0
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
    std::cout << tree.DrawBPlusTree() << std::endl;
  }

  bool is_present;
  std::vector<RID> rids;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 4);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
    std::cout << tree.DrawBPlusTree() << std::endl;
    std::cout << "---------------------------------------------" << std::endl;
  }
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  // 注意：如果一直插入到最左侧，在找key的位置的时候，最小key应该在index=1的位置
  std::vector<int64_t> keys = {10, 20, 30, -2, -10, -20, -30, -40};
  for (auto key : keys) {
    std::cout << "insert: " << key << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    if (key == -30) {
      std::cout << "-30" << std::endl;
    }
    tree.Insert(index_key, rid);
    std::cout << tree.DrawBPlusTree() << std::endl;
  }

  bool is_present;
  std::vector<RID> rids;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    if (key >= 0) {
      EXPECT_EQ(rids[0].GetPageId(), 0);
    } else {
      EXPECT_EQ(rids[0].GetPageId(), -1);
    }
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
    std::cout << tree.DrawBPlusTree() << std::endl;
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    auto pair = *iter;
    auto location = pair.second;
    ASSERT_EQ(location.GetPageId(), 0);
    ASSERT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  ASSERT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  delete bpm;
}
}  // namespace bustub
