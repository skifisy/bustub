//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {}

// 实现点查询就行了
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_idx_ >= plan_->pred_keys_.size()) {
    return false;
  }
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  BUSTUB_ASSERT(index_info != nullptr, "Index must exist");
  const auto &index = index_info->index_;
  //   auto b_plus_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index.get());
  //   BUSTUB_ASSERT(b_plus_tree_index != nullptr, "Index must be BPlusTreeIndexForTwoIntegerColumn");

  Tuple key_tuple;  // 构造索引键的tuple
  while(current_idx_ < plan_->pred_keys_.size()) {
    const auto& col_expr = plan_->pred_keys_[current_idx_++];
    auto val = col_expr->Evaluate(&key_tuple, GetOutputSchema());  // 不需要tuple和schema
    // 获取key schema
    const auto &key_schema = index->GetKeySchema();
    // 仅支持单列索引
    BUSTUB_ASSERT(key_schema->GetColumnCount() == 1, "Only single-column index is supported");
    std::vector<Value> key_values{val};
    key_tuple = Tuple(key_values, key_schema);
    // 查找索引
    std::vector<RID> result;
    index->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
    if (!result.empty()) {
      BUSTUB_ASSERT(result.size() == 1, "Point query should return only one result");
      *rid = result[0];
      // 读取数据表中的tuple
      auto table_info = catalog->GetTable(plan_->table_oid_);
      auto [found, tup] = table_info->table_->GetTuple(*rid);
      if (!found.is_deleted_) {
        *tuple = std::move(tup);
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
