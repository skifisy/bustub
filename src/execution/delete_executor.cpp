//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>

#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_executed_) {
    return false;
  }
  is_executed_ = true;
  table_oid_t tid = plan_->table_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(tid);
  auto table_heap = table->table_.get();

  int ret = 0;
  // 从底层算子获取tuple
  Tuple tup;
  RID r;
  auto txn = exec_ctx_->GetTransaction();
  if (txn != nullptr) {
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    while (child_executor_->Next(&tup, &r)) {
      auto [base_meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_heap, r);
      // 1. 检查write-write冲突
      if (IsWriteWriteConflict(txn, &base_meta)) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict in delete_executor");
      }
      // 2. 自我修改，更新撤销日志
      if (base_meta.ts_ == txn->GetTransactionTempTs()) {
        // 更新撤销日志
        if (link.has_value()) {
          BUSTUB_ASSERT(link->prev_txn_ == txn->GetTransactionId(), "error");
          auto undo_log = txn->GetUndoLog(link->prev_log_idx_);
          auto new_log = GenerateUpdatedUndoLog(&table->schema_, &base_tuple, nullptr, undo_log);
          txn->ModifyUndoLog(link->prev_log_idx_, new_log);
        }
        // 修改数据
        table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, r);
      } else {
        // 3. 其他情况，生成撤销日志，并链接
        UndoLink prev_link = link.has_value() ? *link : UndoLink();
        UndoLog new_log = GenerateNewUndoLog(&table->schema_, &base_tuple, nullptr, base_meta.ts_, prev_link);
        txn->AppendUndoLog(new_log);
        UndoLink new_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum()) - 1};
        // 更新tuple_meta和undo_link
        txn_mgr->UpdateUndoLink(r, new_link);
        table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, r);
        txn->AppendWriteSet(table->oid_, r);
      }

      // 删除索引
      const auto &indexes = catalog->GetTableIndexes(table->name_);
      for (auto &index : indexes) {
        auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
        auto index_key = tup.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        bplus_index->DeleteEntry(index_key, r, exec_ctx_->GetTransaction());
      }
      ret++;
    }
    Value v = ValueFactory::GetIntegerValue(ret);
    std::vector<Value> values{v};
    *tuple = Tuple{values, &GetOutputSchema()};

    return true;
  }

  while (child_executor_->Next(&tup, &r)) {
    // 删除数据
    table_heap->UpdateTupleMeta({time(nullptr), true}, r);

    // 删除索引
    const auto &indexes = catalog->GetTableIndexes(table->name_);
    for (auto &index : indexes) {
      auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
      auto index_key = tup.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      bplus_index->DeleteEntry(index_key, r, exec_ctx_->GetTransaction());
    }
    ret++;
  }
  Value v = ValueFactory::GetIntegerValue(ret);
  std::vector<Value> values{v};
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
