//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstddef>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/catalog.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  table_info_ = catalog->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Nested Index Join logic goes here
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 查index
    std::vector<Value> key_values;
    key_values.emplace_back(plan_->key_predicate_->Evaluate(&child_tuple, plan_->InnerTableSchema()));
    Tuple join_key(key_values, index_info_->index_->GetKeySchema());
    auto &index = index_info_->index_;
    std::vector<RID> result;
    index->ScanKey(join_key, &result, exec_ctx_->GetTransaction());
    if (result.empty() && plan_->join_type_ == JoinType::LEFT) {
      // 连接
      // construct left join tuple
      std::vector<Value> values;
      auto &left_schema = child_executor_->GetOutputSchema();
      auto &right_schema = plan_->InnerTableSchema();
      for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.emplace_back(child_tuple.GetValue(&left_schema, i));
      }
      for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    if (!result.empty()) {
      BUSTUB_ASSERT(result.size() == 1, "error");
      auto [meta, right_tuple] = table_info_->table_->GetTuple(result[0]);
      if (meta.is_deleted_) {
        continue;
      }
      std::vector<Value> values;
      for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
