#include <cstddef>
#include <memory>
#include <optional>
#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/array_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/vector_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/vector_index_scan_plan.h"
#include "fmt/core.h"
#include "optimizer/optimizer.h"
#include "storage/index/hnsw_index.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

struct VectorOptimizeContext {
  explicit VectorOptimizeContext(const Catalog &catalog) : catalog_(catalog) {}

  const Catalog &catalog_;
  table_oid_t table_oid_;
  uint32_t col_idx_;
  VectorExpressionType dist_fn_;
  std::string vector_index_match_method_;
  std::shared_ptr<ArrayExpression> base_expr_;
};

auto FindMatchingVectorIndex(VectorOptimizeContext &ctx) -> std::shared_ptr<IndexInfo> {
  // 不匹配索引，进行精确KNN搜索
  if (ctx.vector_index_match_method_ == "none") {
    return nullptr;
  }

  // table对应列非向量值，无法使用索引
  auto table_info = ctx.catalog_.GetTable(ctx.table_oid_);
  const auto &column = table_info->schema_.GetColumn(ctx.col_idx_);
  if (column.GetType() != TypeId::VECTOR) {
    return nullptr;
  }

  auto indexes = ctx.catalog_.GetTableIndexes(table_info->name_);
  for (auto &index_info : indexes) {
    // 检查索引类型是否匹配
    bool type_matched = false;
    if (ctx.vector_index_match_method_.empty() || ctx.vector_index_match_method_ == "unset") {
      type_matched = (index_info->index_type_ == IndexType::VectorHNSWIndex ||
                      index_info->index_type_ == IndexType::VectorIVFFlatIndex);
    } else if (ctx.vector_index_match_method_ == "hnsw") {
      type_matched = (index_info->index_type_ == IndexType::VectorHNSWIndex);
    } else if (ctx.vector_index_match_method_ == "ivfflat") {
      type_matched = (index_info->index_type_ == IndexType::VectorIVFFlatIndex);
    } else {
      UNIMPLEMENTED("unsupported vector index match method");
      return nullptr;
    }

    if (!type_matched) {
      continue;
    }

    // 检查距离函数是否匹配
    auto *vector_index = reinterpret_cast<VectorIndex *>(index_info->index_.get());
    if (vector_index->distance_fn_ != ctx.dist_fn_) {
      continue;
    }

    // 检查索引列是否匹配
    const auto &key_attrs = vector_index->GetMetadata()->GetKeyAttrs();
    if (key_attrs.size() == 1 && key_attrs.front() == ctx.col_idx_) {
      return index_info;
    }
  }

  return nullptr;
}

// 从排序表达式中提取 base_vector、索引列和距离函数
auto ExtractOrderByExpression(const AbstractExpressionRef &expr, VectorExpressionType &dist_type,
                              std::shared_ptr<ArrayExpression> &base_expr, column_oid_t &col_idx) -> bool {
  auto vector_expr = std::dynamic_pointer_cast<VectorExpression>(expr);
  if (vector_expr == nullptr) {
    return false;
  }

  dist_type = vector_expr->expr_type_;
  auto left_expr = vector_expr->GetChildAt(0);
  auto right_expr = vector_expr->GetChildAt(1);

  auto try_extract = [&base_expr, &col_idx](const AbstractExpressionRef &array_candidate,
                                            const AbstractExpressionRef &col_candidate) -> bool {
    auto base = std::dynamic_pointer_cast<ArrayExpression>(array_candidate);
    if (base == nullptr) {
      return false;
    }
    auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(col_candidate);
    if (col_expr == nullptr) {
      return false;
    }
    base_expr = base;
    col_idx = col_expr->GetColIdx();
    return true;
  };

  return try_extract(left_expr, right_expr) || try_extract(right_expr, left_expr);
}

auto Optimizer::OptimizeAsVectorIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeAsVectorIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 查找 TopN 节点
  if (optimized_plan->GetType() != PlanType::TopN) {
    return optimized_plan;
  }

  const auto &topn_plan = dynamic_cast<const TopNPlanNode &>(*optimized_plan);
  const auto &order_bys = topn_plan.GetOrderBy();

  // 只允许单列索引，只支持升序
  if (order_bys.size() != 1) {
    return optimized_plan;
  }

  const auto &[order_type, order_expr] = order_bys.front();
  BUSTUB_ASSERT(order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC, "expect asc order");

  // 提取距离计算方法、base_vector 和索引列
  VectorOptimizeContext ctx(catalog_);
  ctx.vector_index_match_method_ = vector_index_match_method_;
  if (!ExtractOrderByExpression(order_expr, ctx.dist_fn_, ctx.base_expr_, ctx.col_idx_)) {
    return optimized_plan;
  }

  auto child = topn_plan.GetChildPlan();
  std::shared_ptr<IndexInfo> index = nullptr;

  switch (child->GetType()) {
    case PlanType::SeqScan: {
      const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*child);
      ctx.table_oid_ = seq_plan.GetTableOid();
      index = FindMatchingVectorIndex(ctx);
      if (index == nullptr) {
        break;
      }
      return std::make_shared<VectorIndexScanPlanNode>(seq_plan.output_schema_, ctx.table_oid_, index->table_name_,
                                                       index->index_oid_, index->name_, ctx.base_expr_,
                                                       topn_plan.GetN());
    }

    case PlanType::Projection: {
      const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*child);
      auto proj_child = proj_plan.GetChildPlan();
      if (proj_child->GetType() != PlanType::SeqScan) {
        return optimized_plan;
      }

      const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*proj_child);

      // 找到 projection 对应的列
      if (ctx.col_idx_ >= proj_plan.expressions_.size()) {
        return optimized_plan;
      }
      auto proj_expr = proj_plan.expressions_[ctx.col_idx_];
      auto proj_col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(proj_expr);
      if (proj_col_expr == nullptr) {
        return optimized_plan;
      }

      ctx.col_idx_ = proj_col_expr->GetColIdx();
      ctx.table_oid_ = seq_plan.GetTableOid();
      index = FindMatchingVectorIndex(ctx);
      if (index == nullptr) {
        break;
      }

      auto vector_index_scan_plan =
          std::make_shared<VectorIndexScanPlanNode>(seq_plan.output_schema_, ctx.table_oid_, index->table_name_,
                                                    index->index_oid_, index->name_, ctx.base_expr_, topn_plan.GetN());
      return std::make_shared<ProjectionPlanNode>(proj_plan.output_schema_, proj_plan.expressions_,
                                                  vector_index_scan_plan);
    }

    default:
      // 其他情况不进行优化
      break;
  }

  return optimized_plan;
}

}  // namespace bustub