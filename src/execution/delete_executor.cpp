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
      DeleteTuple(r, table.get(), txn, txn_mgr);
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
