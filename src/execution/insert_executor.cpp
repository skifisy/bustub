//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), is_executed_(false) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_executed_) {
    return false;
  }
  table_oid_t tid = plan_->table_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(tid);
  auto table_heap = table->table_.get();
  // 从子算子获取tuple
  Tuple tup;
  int ret = 0;
  TupleMeta meta = {0, false};
  RID r;
  while (child_executor_->Next(&tup, &r)) {
    meta.ts_ = time(nullptr);
    if (table_heap->InsertTuple(meta, tup)) {
      ret++;
    }
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, ret);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_executed_ = true;
  return true;
}

}  // namespace bustub
