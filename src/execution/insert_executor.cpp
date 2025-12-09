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

#include <cstddef>
#include <memory>

#include "common/bustub_instance.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

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
  std::optional<RID> rid_inserted;
  auto txn = exec_ctx_->GetTransaction();
  if (txn != nullptr) {
    while (child_executor_->Next(&tup, &r)) {
      // step1: 检查主键索引
      const auto &indexes = catalog->GetTableIndexes(table->name_);
      for (auto &index : indexes) {
        if (index->is_primary_key_) {
          auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
          auto index_key = tup.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          std::vector<RID> rids;
          bplus_index->ScanKey(index_key, &rids, txn);
          if (!rids.empty()) {
            // 违反唯一约束，终止事务
            // 检查table_heap中的tuple，可能已经删除了！
            txn->SetTainted();
            throw ExecutionException("the tuple is already exists in the primary key index");
          }
        }
      }

      // step2: 向table_heap中插入数据
      // 1. 设置事务临时时间戳
      meta.ts_ = txn->GetTransactionTempTs();
      // 2. 插入数据
      if ((rid_inserted = table_heap->InsertTuple(meta, tup))) {
        ret++;
        // 2.1 加入到事务的 write set
        txn->AppendWriteSet(tid, *rid_inserted);
        // 2.2 插入索引
        // step3: 实际插入到索引中
        for (auto &index : indexes) {
          auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
          auto index_key = tup.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          bool inserted = bplus_index->InsertEntry(index_key, *rid_inserted, exec_ctx_->GetTransaction());
          if (!inserted) {
            txn->SetTainted();
            throw ExecutionException("the tuple is already exists in the primary key index");
          }
        }
      }
    }
    std::vector<Value> values{};
    values.emplace_back(TypeId::INTEGER, ret);
    *tuple = Tuple{values, &GetOutputSchema()};
    is_executed_ = true;
    return true;
  }

  while (child_executor_->Next(&tup, &r)) {
    meta.ts_ = INVALID_TS;
    if ((rid_inserted = table_heap->InsertTuple(meta, tup))) {
      ret++;
      // 插入索引
      const auto &indexes = catalog->GetTableIndexes(table->name_);
      for (auto &index : indexes) {
        auto bplus_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index->index_.get());
        auto index_key = tup.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        bplus_index->InsertEntry(index_key, *rid_inserted, exec_ctx_->GetTransaction());
      }
    }
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, ret);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_executed_ = true;
  return true;
}

}  // namespace bustub
