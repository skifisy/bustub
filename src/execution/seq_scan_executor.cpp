//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <memory>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"
#include "type/type_id.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t tid = plan_->table_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(tid);
  auto table_heap = table_info_->table_.get();
  iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter = plan_->filter_predicate_;
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto txn = exec_ctx_->GetTransaction();

  while (!iter_->IsEnd()) {
    auto [tup_meta, tup] = iter_->GetTuple();
    *rid = iter_->GetRID();
    ++(*iter_);
    // mvcc
    if (txn != nullptr) {
      auto undo_logs_opt = CollectUndoLogs(*rid, tup_meta, tup, txn_mgr->GetUndoLink(*rid), txn, txn_mgr);
      if (undo_logs_opt.has_value()) {
        auto tup_opt = ReconstructTuple(&table_info_->schema_, tup, tup_meta, *undo_logs_opt);
        if (!tup_opt.has_value()) {
          continue;
        }
        tup = *tup_opt;
      } else {
        continue;
      }
    } else if (tup_meta.is_deleted_) {
      continue;
    }

    *tuple = std::move(tup);
    // 根据过滤条件过滤
    if (filter) {
      auto value = filter->Evaluate(tuple, plan_->OutputSchema());
      switch (value.GetTypeId()) {
        case TypeId::BOOLEAN: {
          if (!static_cast<bool>(value.GetAs<int8_t>())) {
            continue;
          }
        } break;
        default: {
          UNIMPLEMENTED("error");
        } break;
      }
    }
    return true;
  }
  return false;
}

}  // namespace bustub
