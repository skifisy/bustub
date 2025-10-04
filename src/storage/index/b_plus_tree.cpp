#include "storage/index/b_plus_tree.h"
#include <cstddef>
#include <tuple>
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard0 = bpm_->ReadPage(header_page_id_);
  auto header_page = guard0.As<BPlusTreeHeaderPage>();

  auto root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return true;
  }
  ReadPageGuard guard1 = bpm_->ReadPage(root_page_id);
  auto root_page = guard1.As<BPlusTreePage>();
  return root_page->GetSize() <= 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  // 1. 预处理：解析header_page，判空，找到root_page
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  auto root_id = header_page->root_page_id_;
  // 如果 b+tree 为空
  if (root_id == INVALID_PAGE_ID) {
    return false;
  }
  // search场景不需要 ctx.root_page_id_ = root_id;
  ctx.read_set_.emplace_back(bpm_->ReadPage(root_id));
  guard.Drop();  // read操作必然是安全节点，所以可以释放父级的header
  // 2. 从root_page找到叶子节点
  LeafSearchRead(key, ctx);

  // 3. 从叶子节点中找到key->value
  auto &leaf_page = ctx.read_set_.back();
  auto leaf = leaf_page.template As<LeafPage>();

  bool ret = false;
  // TODO(optimize) 二分查找
  // result为数组，表示一个key可能有多个value
  // TODO(bug) 查找的内容必定在该block中吗？假设某个key超过了一个block的容量
  for (int i = 0; i < leaf->GetSize(); i++) {
    if (comparator_(leaf->KeyAt(i), key) == 0) {
      result->emplace_back(leaf->ValueAt(i));
      ret = true;
    }
  }
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafSearchRead(const KeyType &key, Context &ctx) {
  while (true) {
    auto &root_guard = ctx.read_set_.back();
    auto root = root_guard.As<BPlusTreePage>();
    if (root->IsLeafPage()) {
      return;
    }
    // 此时root为内部节点，根据key找到对应的孩子节点
    auto internal_page = reinterpret_cast<const InternalPage *>(root);
    int index = internal_page->SearchKeyIndex(key, comparator_);
    page_id_t child_page_id = internal_page->ValueAt(index);
    ctx.read_set_.emplace_back(bpm_->ReadPage(child_page_id));
    // 安全节点，释放祖先节点
    ctx.read_set_.pop_front();
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  // 1. 解析header
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  auto root_id = header->root_page_id_;
  ctx.root_page_id_ = root_id;
  // 2. 无root节点，创建root
  if (root_id == INVALID_PAGE_ID) {
    root_id = bpm_->NewPage();
    header->root_page_id_ = root_id;
    if (root_id == INVALID_PAGE_ID) {
      return false;
    }
    auto root_page = bpm_->WritePage(root_id);
    auto root = root_page.AsMut<LeafPage>();
    root->Init(leaf_max_size_);
    return root->InsertKeyValue(key, value, comparator_);
  }
  // 3. 获取root_page, 搜索leaf
  ctx.write_set_.emplace_back(bpm_->WritePage(root_id));
  LeafSearch<true>(key, ctx);
  // 3.1 先插入leaf节点
  WritePageGuard leaf_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf = leaf_guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(leaf->IsLeafPage() == true, "is not leaf page");
  // 3.1.1 叶子节点没满，直接插入节点
  int pos = leaf->SearchKeyIndex(key, comparator_);
  if (pos != -1) {
    if (comparator_(leaf->KeyAt(pos), key) == 0) {
      return false;
    }
  }
  if (pos == -1) {
    if (leaf->GetSize() == 0) {
      pos = 0;
    } else {
      BUSTUB_ASSERT(comparator_(leaf->KeyAt(0), key) < 0, "error");
      pos = leaf->GetSize();
    }
  }
  if (!leaf->IsFull()) {
    return leaf->InsertKeyValueByIndex(key, value, pos, comparator_);
  }
  // 3.1.2 叶子节点满，分裂节点
  auto new_page_id = bpm_->NewPage();
  auto new_page = bpm_->WritePage(new_page_id);
  auto new_leaf = new_page.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_page_id);
  leaf->SplitLeafPage(*new_leaf, key, value, comparator_);

  // 3.2 再插入上层的中间节点（循环）
  // 3.2.1 获取向上提升的节点（new page的最左侧节点）
  KeyType new_key = new_leaf->KeyAt(0);
  page_id_t new_value = new_page_id;

  WritePageGuard parent_guard;
  InternalPage *parent = nullptr;
  while (!ctx.write_set_.empty()) {
    parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    parent = parent_guard.AsMut<InternalPage>();
    BUSTUB_ASSERT(!parent->IsLeafPage(), "parent should be internal page");
    // 3.2.2 内部节点非满，直接插入
    if (parent->InsertKeyValue(new_key, new_value, comparator_)) {
      return true;
    }
    // 3.2.3 内部节点满，继续分裂
    auto new_internal_id = bpm_->NewPage();
    auto new_internal_page = bpm_->WritePage(new_internal_id);
    auto new_internal = new_internal_page.AsMut<InternalPage>();
    new_internal->Init(internal_max_size_);
    new_key = parent->SplitInternalPage(*new_internal, new_key, new_value, comparator_);
    new_value = new_internal_id;
  }

  // 4. 一直分裂，直到遇到root节点，需要创建新的root
  auto new_root_id = bpm_->NewPage();
  auto new_root_page = bpm_->WritePage(new_root_id);
  auto new_root = new_root_page.AsMut<InternalPage>();
  new_root->Init(internal_max_size_);
  new_root->SetSize(2);
  new_root->SetKeyAt(1, new_key);
  new_root->SetValueAt(0, ctx.root_page_id_);
  new_root->SetValueAt(1, new_value);
  if (parent == nullptr) {
    new_root->SetKeyAt(0, leaf->KeyAt(0));
  } else {
    new_root->SetKeyAt(0, parent->KeyAt(0));
  }
  BUSTUB_ASSERT(ctx.header_page_.has_value(), "error");
  header->root_page_id_ = new_root_id;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  // 1. 解析header
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  auto root_id = header->root_page_id_;
  ctx.root_page_id_ = root_id;
  // 2. 无root，直接返回
  if (root_id == INVALID_PAGE_ID) {
    return;
  }
  // 3. 获取root_page，搜索leaf
  ctx.write_set_.emplace_back(bpm_->WritePage(root_id));
  LeafSearch<false>(key, ctx);
  // 4. 先在leaf节点中尝试删除
  WritePageGuard leaf_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf = leaf_guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(leaf->IsLeafPage() == true, "is not leaf page");
  // 4.1 leaf节点数量大于一半，直接删除key；或者leaf为root，直接删除
  if (leaf->DeleteKey(key, comparator_, root_id == leaf_guard.GetPageId())) {
    if (leaf->GetSize() == 0) {
      header->root_page_id_ = INVALID_PAGE_ID;
      bpm_->DeletePage(root_id);
    }
    return;
  }
  // 4.2 leaf节点key数量等于一半-1，从兄弟节点借一个条目，或者直接合并
  // leaf数量等于min值，就直接借条目
  BUSTUB_ASSERT(leaf->GetSize() == (leaf->GetMaxSize() + 1) / 2, "error");
  BUSTUB_ASSERT(leaf_guard.GetPageId() != root_id, "error");
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "error");
  WritePageGuard parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent = parent_guard.AsMut<InternalPage>();
  BUSTUB_ASSERT(!parent->IsLeafPage(), "error");
  auto leaf_key = leaf->KeyAt(0);
  int key_index = parent->SearchKeyIndex(leaf_key, comparator_);
  auto sibling_ret = BorrowOrCombineWithSiblingLeafPage(leaf_guard, parent, key, comparator_, key_index);
  if (sibling_ret.first) {
    return;
  }

  // 5. 删除父节点的key-value
  int index_tobe_deleted = sibling_ret.second;
  if (parent->DeleteKeyByIndex(index_tobe_deleted)) {
    return;
  }
  WritePageGuard cur_page_guard;
  // 根据路径，循环删除
  while (!ctx.write_set_.empty()) {
    cur_page_guard = std::move(parent_guard);
    parent_guard = std::move(ctx.write_set_.back());
    parent = parent_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    // 5.2 借条目，合并节点
    auto [is_borrow, key_index, key] = BorrowOrCombineWithSiblingInternalPage(cur_page_guard, parent, comparator_);
    // 处理parent节点
    if (is_borrow) {
      // 更新parent的key值
      parent->SetKeyAt(key_index, key);
      return;
    }
    // 删除parent中的key值
    if (parent->DeleteKeyByIndex(key_index)) {
      return;
    }
  }
  // 6. 合并到根节点，降低树高
  if (parent->GetSize() <= 1) {
    // size减少为0，那么该节点肯定为根节点，删除节点，更新根节点
    BUSTUB_ASSERT(parent_guard.GetPageId() == root_id, "error");
    BUSTUB_ASSERT(ctx.header_page_.has_value(), "error");
    header->root_page_id_ = parent->ValueAt(0);
    page_id_t parent_id = parent_guard.GetPageId();
    parent_guard.Drop();
    bpm_->DeletePage(parent_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowOrCombineWithSiblingLeafPage(WritePageGuard &leaf_guard, InternalPage *parent,
                                                        const KeyType &target_key, KeyComparator &comparator,
                                                        int key_index) -> std::pair<bool, int> {
  auto leaf = leaf_guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(leaf->IsLeafPage(), "error");
  BUSTUB_ASSERT(!parent->IsLeafPage(), "error");
  BUSTUB_ASSERT(leaf->GetSize() <= (leaf->GetMaxSize() + 1) / 2, "error");
  BUSTUB_ASSERT(key_index >= 0, "error");
  // 1. 处理右节点
  if (key_index + 1 < parent->GetSize()) {
    page_id_t right_page_id = parent->ValueAt(key_index + 1);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.AsMut<LeafPage>();
    BUSTUB_ASSERT(right_page->IsLeafPage(), "error");
    // 1.1 右节点能借到一个node
    // 1.1.1 将left的位置空出来
    leaf->DeleteKey(target_key, comparator, true);
    if (right_page->GetSize() + leaf->GetSize() > leaf->GetMaxSize()) {
      // right_page->GetSize() - 1 > (right_page->GetMaxSize() + 1) / 2
      BUSTUB_ASSERT(right_page->GetSize() > (right_page->GetMaxSize() + 2) / 2, "error");
      // 1.1.2 将右节点的首个key借过来
      int insert_pos = leaf->GetSize();
      KeyType right_key = right_page->KeyAt(0);
      leaf->SetSize(insert_pos + 1);
      leaf->SetKeyAt(insert_pos, right_key);
      leaf->SetValueAt(insert_pos, right_page->ValueAt(0));
      right_page->DeleteKey(right_key, comparator, true);
      // 1.1.3 将右节点的新key向上传
      right_key = right_page->KeyAt(0);
      parent->SetKeyAt(key_index + 1, right_key);  // value不变
      return {true, -1};
    }
    BUSTUB_ASSERT(right_page->GetSize() <= (right_page->GetMaxSize() + 2) / 2, "error");
    // 1.2 借不到node直接合并
    // 1.2.1 合并
    leaf->CombinePage(*right_page);
    // 1.2.2 删除right page
    right_page_guard.Drop();
    bpm_->DeletePage(right_page_id);
    // 1.2.3 删除上级的key
    return {false, key_index + 1};
  }
  // 2. 处理左节点
  page_id_t left_page_id = parent->ValueAt(key_index - 1);
  auto left_page_guard = bpm_->WritePage(left_page_id);
  auto left_page = left_page_guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(left_page->IsLeafPage(), "error");
  // 2.1 左节点能借到一个node
  // 先删除当前节点的key
  leaf->DeleteKey(target_key, comparator_, true);
  int left_size = left_page->GetSize();
  if (left_page->GetSize() > (left_page->GetMaxSize() + 2) / 2) {
    // 2.1.1 左节点的key_value插入
    // todo
    leaf->InsertKeyValue(left_page->KeyAt(left_size - 1), left_page->ValueAt(left_size - 1), comparator);
    // 2.1.2 左节点大小-1
    left_page->SetSize(left_size - 1);
    // 2.1.3 右节点新key上传
    parent->SetKeyAt(key_index, leaf->KeyAt(0));
    return {true, -1};
  }
  // 2.2 合并节点
  // 2.2.1 合并
  left_page->CombinePage(*leaf);
  // 2.2.2 删除right_page
  page_id_t leaf_page_id = leaf_guard.GetPageId();
  bpm_->DeletePage(leaf_page_id);
  // 2.2.3 删除上级key
  return {false, key_index};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowOrCombineWithSiblingInternalPage(WritePageGuard &cur_internal_guard, InternalPage *parent,
                                                            KeyComparator &comparator)
    -> std::tuple<bool, int, KeyType> {
  auto cur_internal = cur_internal_guard.AsMut<InternalPage>();
  // 1. 寻找在父节点的index
  int key_index = parent->SearchKeyIndex(cur_internal->KeyAt(0), comparator);
  // 2. 向右节点借用或合并
  int parent_size = parent->GetSize();
  if (key_index < parent_size - 1) {
    // 右节点
    auto right_page_id = parent->ValueAt(key_index + 1);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.template AsMut<InternalPage>();
    // 2.1 判断，尝试借用节点
    int cur_size = cur_internal->GetSize();
    int right_size = right_page->GetSize();
    if (right_size > (right_page->GetMaxSize() + 2) / 2) {
      // 2.1.1 从右节点借用一个
      BUSTUB_ASSERT(right_size > 0, "error");
      cur_internal->SetSize(cur_size + 1);
      cur_internal->SetKeyAt(cur_size, right_page->KeyAt(0));
      cur_internal->SetValueAt(cur_size, right_page->ValueAt(0));
      // 2.1.2 右节点删除借用的节点
      right_page->DeleteKeyByIndex(0);
      // 2.1.3 返回父节点要调整的key_index，key_value
      return {true, key_index + 1, right_page->KeyAt(0)};
    }
    // 2.2 否则，合并右节点
    // 2.2.1 先保存要删除的key
    KeyType key_tobe_delete = right_page->KeyAt(0);
    // 2.2.2 合并节点
    cur_internal->CombinePage(*right_page);
    // 2.2.3 删除right_page
    bpm_->DeletePage(right_page_id);
    return {false, key_index + 1, key_tobe_delete};
  }
  // 3. 向左节点借用/合并
  BUSTUB_ASSERT(key_index - 1 >= 0, "error");
  auto left_page_id = parent->ValueAt(key_index - 1);
  auto left_page_guard = bpm_->WritePage(left_page_id);
  auto left_page = left_page_guard.template AsMut<InternalPage>();

  // 3.1 尝试借用节点
  int left_size = left_page->GetSize();
  if (left_size > (left_page->GetMaxSize() + 2) / 2) {
    // 3.1.1 从左节点借用一个
    BUSTUB_ASSERT(left_size > 0, "error");
    KeyType k = left_page->KeyAt(left_size - 1);
    page_id_t id = left_page->ValueAt(left_size - 1);
    cur_internal->InsertKeyValueByIndex(k, id, 0, comparator_);
    left_page->SetSize(left_size - 1);
    // 3.1.2 更新上层的key
    return {true, key_index, k};
  }
  // 3.2 合并到左侧节点
  // 3.2.1 保存要删除的key
  KeyType key_tobe_delete = cur_internal->KeyAt(0);
  // 3.2.2 合并节点
  left_page->CombinePage(*cur_internal);
  // 3.2.3 删除cur_page
  bpm_->DeletePage(cur_internal_guard.GetPageId());
  return {false, key_index, key_tobe_delete};
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.As<BPlusTreeHeaderPage>();
  if (header->root_page_id_ != INVALID_PAGE_ID) {
    // todo: 不能用read_page吗？
    // 找到最左叶子节点
    page_id_t cur_page_id = header->root_page_id_;

    WritePageGuard guard = bpm_->WritePage(cur_page_id);
    auto page = guard.As<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal_page = guard.As<InternalPage>();
      guard = bpm_->WritePage(internal_page->ValueAt(0));
      page = guard.As<BPlusTreePage>();
    }
    return INDEXITERATOR_TYPE(std::move(guard), 0, bpm_);
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 先处理header节点
  ReadPageGuard header_page = bpm_->ReadPage(header_page_id_);
  auto header = header_page.As<BPlusTreeHeaderPage>();
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  auto cur_page_id = header->root_page_id_;
  // 当迭代到叶子节点时，需要持有父节点，然后将叶子节点的读锁提升为写锁
  ReadPageGuard cur = bpm_->ReadPage(cur_page_id);
  ReadPageGuard parent = std::move(header_page);
  auto cur_node = cur.As<BPlusTreePage>();
  // 这里使用parent的目的是为了保护parent节点，防止被writer更改
  // 所以，直接用类型无关的PageGuard保存即可
  while (!cur_node->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_node);
    int index = internal_page->SearchKeyIndex(key, comparator_);
    cur_page_id = internal_page->ValueAt(index);
    parent = std::move(cur);
    cur = bpm_->ReadPage(cur_page_id);
    cur_node = cur.As<BPlusTreePage>();
  }
  auto leaf = reinterpret_cast<const LeafPage *>(cur_node);
  int pos = leaf->SearchKeyIndex(key, comparator_);
  if (pos == -1) {
    return INDEXITERATOR_TYPE();
  }
  // 读锁提升为写锁
  cur.Drop();
  return INDEXITERATOR_TYPE(bpm_->WritePage(cur_page_id), pos, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
