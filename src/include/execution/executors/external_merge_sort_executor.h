//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

#define SORT_PAGE_HEADER_SIZE (sizeof(size_t) * 2)
#define SORT_PAGE_DATA_SIZE (BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER_SIZE)
/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  explicit SortPage(size_t tuple_size) : tuple_size_(tuple_size) {}
  void Init(size_t tuple_size) {
    count_ = 0;
    tuple_size_ = tuple_size + sizeof(int32_t);  // include the size field
  }
  auto GetCount() const -> size_t { return count_; }
  void SetCount(size_t count) { count_ = count; }
  void SerializeTuples(const std::vector<Tuple> &tuples) {
    BUSTUB_ASSERT(tuples.size() <= GetMaxCount(), "error");
    count_ = tuples.size();
    char *cur_ptr = data_;
    for (const auto &tup : tuples) {
      BUSTUB_ASSERT(tup.GetLength() + sizeof(int32_t) == tuple_size_, "error");
      tup.SerializeTo(cur_ptr);
      cur_ptr += tuple_size_;
    }
  }
  auto GetTupleAt(size_t idx) const -> Tuple {
    char *ptr = data_ + idx * tuple_size_;
    Tuple tup;
    tup.DeserializeFrom(ptr);
    return tup;
  }
  auto GetMaxCount() const -> size_t { return SORT_PAGE_DATA_SIZE / tuple_size_; }
  static auto GetMaxCount(size_t tuple_size) -> size_t { return SORT_PAGE_DATA_SIZE / (tuple_size + sizeof(int32_t)); }

 private:
  size_t count_{0};       // tuple数量
  size_t tuple_size_{0};  // 单个tuple大小
  char *data_{nullptr};   // 具体数据
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 * 一个run：表示一个归并段，（将输入划分为多个排序好的block）
 * 一个run可能占用多个page
 * 单页有序&多页之间也是有序的
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }

  auto GetPages() -> std::vector<page_id_t> & { return pages_; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * 后缀增加
     */
    auto operator++() -> Iterator & {
      BUSTUB_ASSERT(is_valid_, "iterator is not valid");
      tuple_idx_++;
      if (tuple_idx_ >= sort_page_->GetCount()) {
        // 移动到下一个page
        page_idx_++;
        if (page_idx_ >= run_->pages_.size()) {
          is_valid_ = false;
          return *this;
        }
        auto pid = run_->pages_[page_idx_];
        guard_ = run_->bpm_->ReadPage(pid);
        tuple_idx_ = 0;
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     */
    auto operator*() -> Tuple {
      BUSTUB_ASSERT(is_valid_, "iterator is not valid");
      BUSTUB_ASSERT(tuple_idx_ < sort_page_->GetCount(), "error");
      return sort_page_->GetTupleAt(tuple_idx_);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * 简单实现
     */
    auto operator==(const Iterator &other) const -> bool { return is_valid_ == other.is_valid_; }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool { return is_valid_ != other.is_valid_; }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run), is_valid_(true) {
      if (run->pages_.empty()) {
        is_valid_ = false;
        return;
      }
      guard_ = run->bpm_->ReadPage(run->pages_[0]);
      sort_page_ = guard_.As<SortPage>();
    }

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};
    bool is_valid_{false};
    size_t page_idx_{0};   // 当前位于run的哪个page
    size_t tuple_idx_{0};  // 位于当前page的哪个tuple
    ReadPageGuard guard_;
    const SortPage *sort_page_{nullptr};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator { return Iterator(this); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator { return {}; }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<std::shared_ptr<MergeSortRun>> runs_;

  MergeSortRun::Iterator iter_;
};

}  // namespace bustub
