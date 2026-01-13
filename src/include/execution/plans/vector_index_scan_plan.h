#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/array_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

class VectorIndexScanPlanNode : public AbstractPlanNode {
 public:
  VectorIndexScanPlanNode(SchemaRef output, table_oid_t table_oid, std::string table_name, index_oid_t index_oid,
                          std::string index_name, std::shared_ptr<const ArrayExpression> base_vector, size_t limit)
      : AbstractPlanNode(std::move(output), {}),
        table_oid_(table_oid),
        table_name_(std::move(table_name)),
        index_oid_(index_oid),
        index_name_(std::move(index_name)),
        base_vector_(std::move(base_vector)),
        limit_(limit) {}

  auto GetType() const -> PlanType override { return PlanType::VectorIndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(VectorIndexScanPlanNode);

  /** The table which the index is created on. */
  table_oid_t table_oid_;
  std::string table_name_;

  /** The index whose tuples should be scanned. */
  index_oid_t index_oid_;
  std::string index_name_;

  std::shared_ptr<const ArrayExpression> base_vector_;

  size_t limit_;

  // Add anything you want here for index lookup

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format(
        "VectorIndexScan {{ index_oid={}, index_name={}, table_oid={}, table_name={} base_vector={}, limit={} }}",
        index_oid_, index_name_, table_oid_, table_name_, base_vector_, limit_);
  }
};

}  // namespace bustub
