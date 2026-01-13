#include "execution/executors/topn_executor.h"
#include <utility>
#include "common/rid.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      heap_(SortComparator(plan->GetOrderBy(), GetOutputSchema())) {}

void TopNExecutor::Init() {
  if (!sorted_tuples_.empty()) {
    iter_ = sorted_tuples_.rbegin();
    return;
  }
  child_executor_->Init();
  RID child_rid;
  Tuple child_tuple;
  // step1: 形成大小为k的大顶堆
  for (size_t i = 0; i < plan_->GetN(); i++) {
    if (child_executor_->Next(&child_tuple, &child_rid)) {
      heap_.emplace(std::pair<Tuple, RID>(std::move(child_tuple), child_rid));
    } else {
      break;
    }
  }

  // step2: 维护大顶堆
  SortComparator comp(plan_->GetOrderBy(), GetOutputSchema());
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto &top = heap_.top();
    std::pair<Tuple, RID> child(std::move(child_tuple), child_rid);
    if (comp(child, top)) {
      heap_.pop();
      heap_.emplace(std::move(child));
    }
  }
  // step3: 弹出所有数据
  sorted_tuples_.reserve(heap_.size());
  while (!heap_.empty()) {
    sorted_tuples_.emplace_back(heap_.top());
    heap_.pop();
  }
  iter_ = sorted_tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_tuples_.rend()) {
    return false;
  }
  *tuple = iter_->first;
  *rid = iter_->second;
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
