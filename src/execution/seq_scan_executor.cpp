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
#include <memory>
#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "type/type_id.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t tid = plan_->table_oid_;
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(tid);
  auto table_heap = table->table_.get();
  iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
  table_info_ = table;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter = plan_->filter_predicate_;
  while (!iter_->IsEnd()) {
    auto [tup_meta, tup] = iter_->GetTuple();
    *rid = iter_->GetRID();
    ++(*iter_);
    if (tup_meta.is_deleted_) {
      continue;
    }
    *tuple = std::move(tup);
    // 根据过滤条件过滤
    if (filter) {
      auto value = filter->Evaluate(tuple, table_info_->schema_);
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
