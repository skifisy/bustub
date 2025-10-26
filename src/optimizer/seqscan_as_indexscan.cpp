#include <cstddef>
#include <memory>
#include <tuple>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
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

  auto is_index = [this](const std::string &table_name, const Schema &schema, const AbstractExpressionRef &expr,
                         std::shared_ptr<IndexInfo> &index_info) -> std::tuple<bool, uint32_t> {
    auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr);
    if (col_expr == nullptr) {
      return {false, 0};
    }
    auto col_name = schema.GetColumn(col_expr->GetColIdx()).GetName();
    auto indexs = catalog_.GetTableIndexes(table_name);
    // 遍历查询当前列是否有索引
    for (auto &index : indexs) {
      auto key_attrs = index->index_->GetKeyAttrs();
      if (std::find(key_attrs.begin(), key_attrs.end(), col_expr->GetColIdx()) != key_attrs.end()) {
        std::cout << "Found index on column " << col_name << " in table " << table_name << "\n";
        index_info = index;
        return {true, col_expr->GetColIdx()};
      }
    }
    return {false, col_expr->GetColIdx()};
  };

  auto can_convert_to_index = [is_index](const std::string &table_name, const Schema &schema,
                                         const AbstractExpressionRef &expr, std::shared_ptr<IndexInfo> &index_info,
                                         std::vector<AbstractExpressionRef> &pred_keys) -> std::tuple<bool, uint32_t> {
    auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr);
    if (comparison_expr == nullptr) {
      return {false, 0};
    }
    if (comparison_expr->comp_type_ != ComparisonType::Equal) {
      return {false, 0};
    }
    auto left_expr = comparison_expr->GetChildAt(0);
    auto right_expr = comparison_expr->GetChildAt(1);
    auto [is_left_index, left_col_idx] = is_index(table_name, schema, left_expr, index_info);
    auto [is_right_index, right_col_idx] = is_index(table_name, schema, right_expr, index_info);
    if ((is_left_index && is_right_index) || (!is_left_index && !is_right_index)) {
      // 两边都是索引，无法转换
      return {false, 0};
    }
    pred_keys.push_back(is_left_index ? right_expr : left_expr);
    return {true, is_left_index ? left_col_idx : right_col_idx};
  };

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      // 如果 SeqScan 有过滤谓词，则可以尝试将其转换为索引扫描
      // TODO() 先简单地判断过滤谓词是否为单个 ColumnValueExpression，不进行递归
      const auto &schema = catalog_.GetTable(seq_scan_plan.table_oid_)->schema_;
      const auto &table_name = seq_scan_plan.table_name_;
      std::vector<AbstractExpressionRef> pred_keys;

      auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(seq_scan_plan.filter_predicate_);
      // 支持形如 col = value OR col = value 的谓词转换为索引扫描
      if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
        std::shared_ptr<IndexInfo> index_info1 = nullptr;
        std::shared_ptr<IndexInfo> index_info2 = nullptr;
        auto [can_convert1, index_key_idx1] =
            can_convert_to_index(table_name, schema, logic_expr->GetChildAt(0), index_info1, pred_keys);
        auto [can_convert2, index_key_idx2] =
            can_convert_to_index(table_name, schema, logic_expr->GetChildAt(1), index_info2, pred_keys);
        if (can_convert1 && can_convert2 && index_key_idx1 == index_key_idx2) {
          BUSTUB_ASSERT(index_info1 != nullptr, "Index must exist");
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                     index_info1->index_oid_, seq_scan_plan.filter_predicate_,
                                                     std::move(pred_keys));
        }
      }

      std::shared_ptr<IndexInfo> index_info = nullptr;
      auto [can_convert, index_key_idx] =
          can_convert_to_index(table_name, schema, seq_scan_plan.filter_predicate_, index_info, pred_keys);
      if (can_convert) {
        BUSTUB_ASSERT(index_info != nullptr, "Index must exist");
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                   index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                                   std::move(pred_keys));
      }
    }
  }
  return plan;
}

}  // namespace bustub
