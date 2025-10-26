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

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  BUSTUB_ASSERT(index_info != nullptr, "Index must exist");
  const auto &index = index_info->index_;
  b_plus_tree_index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index.get());
  BUSTUB_ASSERT(b_plus_tree_index_ != nullptr, "Index must be BPlusTreeIndexForTwoIntegerColumn");

  // group_by查询
  if (plan_->pred_keys_.empty()) {
    is_point_scan_ = false;
    index_iter_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(b_plus_tree_index_->GetBeginIterator());
  }
}

// 实现点查询就行了
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_point_scan_ && current_idx_ >= plan_->pred_keys_.size()) {
    return false;
  }

  if (is_point_scan_) {
    Tuple key_tuple;  // 构造索引键的tuple
    while (current_idx_ < plan_->pred_keys_.size()) {
      const auto &col_expr = plan_->pred_keys_[current_idx_++];
      auto val = col_expr->Evaluate(&key_tuple, GetOutputSchema());  // 不需要tuple和schema
      // 获取key schema
      const auto &key_schema = b_plus_tree_index_->GetKeySchema();
      // 仅支持单列索引
      BUSTUB_ASSERT(key_schema->GetColumnCount() == 1, "Only single-column index is supported");
      std::vector<Value> key_values{val};
      key_tuple = Tuple(key_values, key_schema);
      // 查找索引
      std::vector<RID> result;
      b_plus_tree_index_->ScanKey(key_tuple, &result, exec_ctx_->GetTransaction());
      if (!result.empty()) {
        BUSTUB_ASSERT(result.size() == 1, "Point query should return only one result");
        *rid = result[0];
        // 读取数据表中的tuple
        auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
        auto [found, tup] = table_info->table_->GetTuple(*rid);
        if (!found.is_deleted_) {
          *tuple = std::move(tup);
          return true;
        }
      }
    }
  } else {
    while (!index_iter_->IsEnd()) {
      // 跳过删除的记录
      auto [key, rid_value] = **index_iter_;
      ++(*index_iter_);
      *rid = rid_value;
      auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
      auto [found, tup] = table_info->table_->GetTuple(*rid);
      if (found.is_deleted_) {
        continue;
      }
      *tuple = std::move(tup);
      return true;
    }
  }

  return false;
}

}  // namespace bustub
