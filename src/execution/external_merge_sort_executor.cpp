//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <memory>
#include <optional>
#include <vector>
#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

void SortTuples(std::vector<Tuple> &tuples, TupleComparator &comp, const std::vector<OrderBy> &order_bys,
                const Schema &schema) {
  std::sort(tuples.begin(), tuples.end(), [&order_bys, &schema, &comp](const Tuple &t1, const Tuple &t2) {
    auto k1 = GenerateSortKey(t1, order_bys, schema);
    auto k2 = GenerateSortKey(t2, order_bys, schema);
    return comp(SortEntry{k1, t1}, SortEntry{k2, t2});
  });
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  if (!runs_.empty()) {
    BUSTUB_ASSERT(runs_.size() == 1, "error");
    iter_ = runs_.front()->Begin();
    return;
  }

  child_executor_->Init();
  // 排序完毕之后才能进入next

  // step1: 生成1 page runs，即排序好并写入磁盘
  Tuple tup;
  RID r;
  size_t tuple_size = child_executor_->GetOutputSchema().GetInlinedStorageSize();
  size_t max_tuple_count = SortPage::GetMaxCount(tuple_size);
  auto bpm = exec_ctx_->GetBufferPoolManager();
  auto &order_bys = plan_->GetOrderBy();
  const Schema &schema = child_executor_->GetOutputSchema();
  TupleComparator comp(order_bys);
  std::vector<Tuple> tuples;

  auto write_page = [&bpm, &tuples](std::shared_ptr<MergeSortRun> &cur_run) {
    // page满，写入
    page_id_t pid = bpm->NewPage();
    cur_run->GetPages().emplace_back(pid);
    auto guard = bpm->WritePage(pid);
    auto sort_page = guard.AsMut<SortPage>();
    sort_page->Init(tuples.front().GetLength());
    sort_page->SerializeTuples(tuples);
    tuples.clear();
  };

  while (child_executor_->Next(&tup, &r)) {
    // 判断page是否满
    if (tuples.size() < max_tuple_count) {
      tuples.emplace_back(std::move(tup));
      continue;
    }
    SortTuples(tuples, comp, order_bys, schema);
    std::shared_ptr<MergeSortRun> cur_run = std::make_shared<MergeSortRun>(std::vector<page_id_t>{}, bpm);
    write_page(cur_run);
    runs_.emplace_back(cur_run);
    tuples.emplace_back(std::move(tup));
  }
  if (!tuples.empty()) {
    SortTuples(tuples, comp, order_bys, schema);
    std::shared_ptr<MergeSortRun> cur_run = std::make_shared<MergeSortRun>(std::vector<page_id_t>{}, bpm);
    write_page(cur_run);
    runs_.emplace_back(cur_run);
  }
  if (runs_.empty()) {
    return;
  }

  // step2: 归并排序
  tuples.clear();
  while (runs_.size() > 1) {
    std::vector<std::shared_ptr<MergeSortRun>> old_runs;
    old_runs.swap(runs_);
    for (size_t start_idx = 0; start_idx < old_runs.size(); start_idx += K) {
      // 完成一次归并
      std::shared_ptr<MergeSortRun> &r1 = old_runs[start_idx];
      if (start_idx + 1 >= old_runs.size()) {
        runs_.emplace_back(r1);
        break;
      }
      std::shared_ptr<MergeSortRun> &r2 = old_runs[start_idx + 1];
      std::shared_ptr<MergeSortRun> cur_run = std::make_shared<MergeSortRun>(std::vector<page_id_t>{}, bpm);
      // 归并
      auto iter1 = r1->Begin();
      auto iter2 = r2->Begin();
      while (iter1 != r1->End() && iter2 != r2->End()) {
        auto t1 = *iter1;
        auto t2 = *iter2;
        auto k1 = GenerateSortKey(t1, order_bys, schema);
        auto k2 = GenerateSortKey(t2, order_bys, schema);
        if (tuples.size() >= max_tuple_count) {
          write_page(cur_run);
        }
        if (comp(SortEntry(k1, t1), SortEntry(k2, t2))) {
          tuples.emplace_back(std::move(t1));
          ++iter1;
        } else {
          tuples.emplace_back(std::move(t2));
          ++iter2;
        }
      }
      while (iter1 != r1->End()) {
        if (tuples.size() >= max_tuple_count) {
          write_page(cur_run);
        }
        tuples.emplace_back(*iter1);
        ++iter1;
      }
      while (iter2 != r2->End()) {
        if (tuples.size() >= max_tuple_count) {
          write_page(cur_run);
        }
        tuples.emplace_back(*iter2);
        ++iter2;
      }
      if (!tuples.empty()) {
        write_page(cur_run);
      }
      runs_.emplace_back(cur_run);
    }
    for (auto &run : old_runs) {
      for (auto pid : run->GetPages()) {
        bpm->DeletePage(pid);
      }
    }
  }
  BUSTUB_ASSERT(runs_.size() == 1, "error");
  iter_ = runs_.front()->Begin();
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(K == 2, "only support for 2 way sort");
  if (runs_.empty()) {
    return false;
  }
  BUSTUB_ASSERT(runs_.size() == 1, "error");
  auto &run = runs_.front();
  if (iter_ != run->End()) {
    *tuple = *iter_;
    ++iter_;
    return true;
  }
  return false;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
