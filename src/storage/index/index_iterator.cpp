/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return is_invalid_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  BUSTUB_ASSERT(!is_invalid_, "error");
  BUSTUB_ASSERT(pos_ >= 0, "error");
  BUSTUB_ASSERT(pos_ < cur_page_->GetSize(), "error");
  return {cur_page_->KeyAt(pos_), cur_page_->ValueAt(pos_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(!is_invalid_, "error");
  if (++pos_ == cur_page_->GetSize()) {
    page_id_t next_page_id = cur_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      is_invalid_ = true;
      read_guard_.Drop();
      pos_ = 0;
      return *this;
    }
    read_guard_ = bpm_->ReadPage(next_page_id);
    cur_page_ = read_guard_.As<LeafPage>();
    pos_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
