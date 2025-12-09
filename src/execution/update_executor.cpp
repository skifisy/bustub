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
#include <cstdint>
#include <memory>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

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
  Tuple old_tup;
  RID r;
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn != nullptr) {
    TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
    while (child_executor_->Next(&old_tup, &r)) {
      // 获取新值
      std::vector<Value> update_values;
      for (auto &target_expr : plan_->target_expressions_) {
        update_values.push_back(target_expr->Evaluate(&old_tup, schema));
      }
      Tuple new_tup(update_values, &schema);
      UpdateTuple(r, new_tup, table.get(), catalog, txn, txn_mgr);
      ret++;
    }
    Value v = ValueFactory::GetIntegerValue(ret);
    std::vector<Value> values{v};
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }

  while (child_executor_->Next(&old_tup, &r)) {
    // 先保存更新后的值（没有被更新的字段用ColumnValueExpression表示，直接取原值）
    std::vector<Value> update_values;
    for (auto &target_expr : plan_->target_expressions_) {
      update_values.push_back(target_expr->Evaluate(&old_tup, schema));
    }
    // 删除原来的数据
    table_heap->UpdateTupleMeta({time(nullptr), true}, r);
    // 插入更新后的数据
    Tuple new_tup(update_values, &schema);
    auto rid_inserted = table_heap->InsertTuple({time(nullptr), false}, new_tup);

    // 更新索引
    const auto &indexes = catalog->GetTableIndexes(table->name_);
    for (auto &index : indexes) {
      auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
      auto index_key = old_tup.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      bplus_index->DeleteEntry(index_key, r, exec_ctx_->GetTransaction());

      auto new_index_key = new_tup.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      bplus_index->InsertEntry(new_index_key, *rid_inserted, exec_ctx_->GetTransaction());
    }
    ret++;
  }
  Value v = ValueFactory::GetIntegerValue(ret);
  std::vector<Value> values{v};
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
