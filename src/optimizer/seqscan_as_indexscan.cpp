#include <cstddef>
#include <memory>
#include <tuple>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 当前规则在OptimizeMergeFilterScan之后，所以直接找到PlanType::SeqScan类型的节点即可
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      const auto &schema = catalog_.GetTable(seq_scan_plan.table_oid_)->schema_;
      const auto &table_name = seq_scan_plan.table_name_;
      auto indexs = catalog_.GetTableIndexes(table_name);
      bool use_index = true;
      std::optional<index_oid_t> index_idx = std::nullopt;
      std::vector<AbstractExpressionRef> pred_keys;
      OptimizeSeqScanAsIndexScanHelper(indexs, schema, seq_scan_plan.filter_predicate_, use_index, pred_keys,
                                       index_idx);
      if (use_index) {
        BUSTUB_ASSERT(index_idx.has_value(), "Index oid must be set if use_index is true");
        // 构造IndexScanPlanNode
        auto index_scan_plan = std::make_shared<IndexScanPlanNode>(
            seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_idx.value(), seq_scan_plan.filter_predicate_,
            std::move(pred_keys));
        return index_scan_plan;
      }
    }
  }

  return optimized_plan;
}

void Optimizer::OptimizeSeqScanAsIndexScanHelper(const std::vector<std::shared_ptr<IndexInfo>> &indexs,
                                                 const Schema &schema, const AbstractExpressionRef &expr,
                                                 bool &use_index, std::vector<AbstractExpressionRef> &pred_keys,
                                                 std::optional<index_oid_t> &index_idx) {
  if (!use_index) {
    return;
  }

  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(expr);
  if (logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      // 递归处理子表达式
      for (const auto &child : logic_expr->GetChildren()) {
        OptimizeSeqScanAsIndexScanHelper(indexs, schema, child, use_index, pred_keys, index_idx);
        if (!use_index) {
          return;
        }
      }
      return;
    }
    use_index = false;
    return;
  }

  auto is_index = [](const Schema &schema, const AbstractExpressionRef &expr,
                     const std::vector<std::shared_ptr<IndexInfo>> &indexs) -> std::tuple<bool, index_oid_t> {
    auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr);
    if (col_expr == nullptr) {
      return {false, 0};
    }
    // 遍历查询当前列是否有索引
    for (auto &index : indexs) {
      auto key_attrs = index->index_->GetKeyAttrs();
      if (std::find(key_attrs.begin(), key_attrs.end(), col_expr->GetColIdx()) != key_attrs.end()) {
        return {true, index->index_oid_};
      }
    }
    return {false, 0};
  };

  auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr);
  if (comparison_expr != nullptr) {
    if (comparison_expr->comp_type_ != ComparisonType::Equal) {
      use_index = false;
      return;
    }
    auto left_expr = comparison_expr->GetChildAt(0);
    auto right_expr = comparison_expr->GetChildAt(1);

    auto [is_left_index, left_index_oid] = is_index(schema, left_expr, indexs);
    auto [is_right_index, right_index_oid] = is_index(schema, right_expr, indexs);
    if ((is_left_index && is_right_index) || (!is_left_index && !is_right_index)) {
      // 两边都是索引，无法转换
      use_index = false;
      return;
    }
    auto idx = is_left_index ? left_index_oid : right_index_oid;
    if (!index_idx.has_value()) {
      index_idx = idx;
      pred_keys.push_back(is_left_index ? right_expr : left_expr);
    } else {
      if (index_idx.value() != idx) {
        // 不同的索引，无法转换
        use_index = false;
      } else {
        pred_keys.push_back(is_left_index ? right_expr : left_expr);
      }
    }
    return;
  }

  use_index = false;
}

}  // namespace bustub
