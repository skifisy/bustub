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
      // 获取旧值
      auto [base_meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_heap, r);
      // 获取新值
      std::vector<Value> update_values;
      for (auto &target_expr : plan_->target_expressions_) {
        update_values.push_back(target_expr->Evaluate(&old_tup, schema));
      }
      Tuple new_tup(update_values, &schema);

      // 1. 检查write-write冲突
      if (IsWriteWriteConflict(txn, &base_meta)) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict in delete_executor");
      }
      // 2. 自我修改，更新撤销日志
      if (base_meta.ts_ == txn->GetTransactionTempTs()) {
        if (link.has_value()) {
          BUSTUB_ASSERT(link->prev_txn_ == txn->GetTransactionId(), "error");
          auto undo_log = txn->GetUndoLog(link->prev_log_idx_);
          auto new_log = GenerateUpdatedUndoLog(&table->schema_, &base_tuple, &new_tup, undo_log);
          txn->ModifyUndoLog(link->prev_log_idx_, new_log);
        }
        // 修改table_heap中的数据
        table_heap->UpdateTupleInPlace(base_meta, new_tup, r);
      } else {
        // 3. 其他情况，生成撤销日志，并链接
        UndoLink prev_link = link.has_value() ? *link : UndoLink();
        UndoLog new_log = GenerateNewUndoLog(&table->schema_, &base_tuple, &new_tup, base_meta.ts_, prev_link);
        txn->AppendUndoLog(new_log);
        UndoLink new_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum()) - 1};
        // 更新tuple和link
        UpdateTupleAndUndoLink(txn_mgr, r, new_link, table_heap, txn, {txn->GetTransactionTempTs(), false}, new_tup);
        txn->AppendWriteSet(table->oid_, r);
      }

      // // 更新索引
      // const auto &indexes = catalog->GetTableIndexes(table->name_);
      // for (auto &index : indexes) {
      //   auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
      //   auto index_key = old_tup.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      //   bplus_index->DeleteEntry(index_key, r, exec_ctx_->GetTransaction());

      //   auto new_index_key = new_tup.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      //   bplus_index->InsertEntry(new_index_key, *rid_inserted, exec_ctx_->GetTransaction());
      // }
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
