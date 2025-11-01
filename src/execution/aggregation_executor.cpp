//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  if (!aht_.IsEmpty()) {
    aht_iterator_ = aht_.Begin();
    return;
  }
  child_executor_->Init();
  // 构建hash table
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);  // 对于没有 group by 的情况，相当于所有数据都在一个组里，即都具有相同的key
    auto val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, val);
  }
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    // 处理没有输入数据的情况，仍然需要输出一行结果
    auto initial_key = AggregateKey{{}};
    auto initial_val = aht_.GenerateInitialAggregateValue();
    aht_.InsertCombine(initial_key, initial_val);
  }
  aht_iterator_ = aht_.Begin();  // 重置迭代器
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();
    ++aht_iterator_;
    std::vector<Value> result_values;
    // 构造输出 tuple 的值
    for (const auto &group_by_val : key.group_bys_) {
      result_values.emplace_back(group_by_val);
    }
    for (const auto &agg_val : value.aggregates_) {
      result_values.emplace_back(agg_val);
    }
    *tuple = Tuple(result_values, &plan_->OutputSchema());
    return true;  // 返回一个结果
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
