//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "concurrency/transaction.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_executed_) {
    return false;
  }
  is_executed_ = true;

  table_oid_t tid = plan_->table_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(tid);
  auto table_heap = table->table_.get();

  auto &schema = table->schema_;

  int ret = 0;
  // 从底层算子获取tuple
  Tuple tup;
  RID r;
  child_executor_->Init();
  while (child_executor_->Next(&tup, &r)) {
    // 先保存更新后的值
    std::vector<Value> update_values;
    for (auto &target_expr : plan_->target_expressions_) {
      update_values.push_back(target_expr->Evaluate(&tup, schema));
    }
    // 删除原来的数据
    table_heap->UpdateTupleMeta({time(nullptr), true}, r);
    // 插入更新后的数据
    Tuple new_tup(update_values, &schema);
    table_heap->InsertTuple({time(nullptr), false}, new_tup);
    ret++;
  }
  Value v = ValueFactory::GetIntegerValue(ret);
  std::vector<Value> values{v};
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
