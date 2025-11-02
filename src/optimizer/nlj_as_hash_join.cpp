#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

struct OHashJoinContext {
  AbstractExpressionRef predicate_;  // 连接条件
  AbstractPlanNodeRef left_;         // 左表
  AbstractPlanNodeRef right_;
  /** The expression to compute the left JOIN key */
  std::vector<AbstractExpressionRef> left_key_expressions_;
  /** The expression to compute the right JOIN key */
  std::vector<AbstractExpressionRef> right_key_expressions_;
  bool use_hash_join_;
};

void OptimizeHashJoin(OHashJoinContext &ctx) {
  if (!ctx.use_hash_join_) {
    return;
  }

  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(ctx.predicate_);
  if (logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      ctx.use_hash_join_ = false;
      return;
    }
    BUSTUB_ASSERT(logic_expr->GetChildren().size() == 2, "logic expr should have 2 children");
    ctx.predicate_ = logic_expr->GetChildAt(0);
    OptimizeHashJoin(ctx);
    ctx.predicate_ = logic_expr->GetChildAt(1);
    OptimizeHashJoin(ctx);
    return;
  }

  auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(ctx.predicate_);
  if (comparison_expr != nullptr) {
    if (comparison_expr->comp_type_ != ComparisonType::Equal) {
      ctx.use_hash_join_ = false;
      return;
    }
    BUSTUB_ASSERT(comparison_expr->children_.size() == 2, "comparison expr should have 2 children");

    auto left_col_exp = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(0));
    auto right_col_exp = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(1));
    BUSTUB_ASSERT(left_col_exp != nullptr, "error");
    BUSTUB_ASSERT(right_col_exp != nullptr, "error");
    if (left_col_exp->GetTupleIdx() == 0) {
      ctx.left_key_expressions_.emplace_back(left_col_exp);
    } else {
      ctx.right_key_expressions_.emplace_back(left_col_exp);
    }
    if (right_col_exp->GetTupleIdx() == 0) {
      ctx.left_key_expressions_.emplace_back(right_col_exp);
    } else {
      ctx.right_key_expressions_.emplace_back(right_col_exp);
    }
    BUSTUB_ASSERT(ctx.left_key_expressions_.size() == ctx.right_key_expressions_.size(), "error");
    return;
  }

  ctx.use_hash_join_ = false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  // 先找到NLJ嵌套循环连接
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    OHashJoinContext context = {nlj_plan.Predicate(), nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), {}, {}, true};
    OptimizeHashJoin(context);
    if(context.use_hash_join_) {
      return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
        std::move(context.left_key_expressions_), std::move(context.right_key_expressions_), nlj_plan.GetJoinType()
      );
    }
  }

  return plan;
}

}  // namespace bustub
