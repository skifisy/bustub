//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "type/type_id.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  left_tuple_valid_ = left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Implement the nested loop join logic here
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  while (left_tuple_valid_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto join_predicate = plan_->Predicate();
      auto can_join = join_predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      switch (can_join.GetTypeId()) {
        case TypeId::BOOLEAN:
          if (can_join.GetAs<bool>()) {
            // construct joined tuple
            std::vector<Value> values;
            for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }
            *tuple = Tuple(values, &plan_->OutputSchema());
            right_tuple_matched_ = true;
            return true;
          }
          break;
        default:
          throw Exception("join predicate must return boolean");
      }
    }
    if (!right_tuple_matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      // construct left join tuple
      std::vector<Value> values;
      auto &left_schema = left_executor_->GetOutputSchema();
      auto &right_schema = right_executor_->GetOutputSchema();
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      right_tuple_matched_ = false;
      left_tuple_valid_ = left_executor_->Next(&left_tuple_, &left_rid);
      right_executor_->Init();
      return true;
    }
    right_tuple_matched_ = false;
    left_tuple_valid_ = left_executor_->Next(&left_tuple_, &left_rid);
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
