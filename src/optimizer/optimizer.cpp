#include "optimizer/optimizer.h"
#include <optional>
#include "common/util/string_util.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {
auto GetSessionVariable(const std::unordered_map<std::string, std::string> &session_variables, const std::string &key)
    -> std::string {
  if (auto iter = session_variables.find(key); iter != session_variables.end()) {
    return iter->second;
  }
  return "";
}

auto IsForceStarterRule(const std::string &option) -> bool {
  auto variable = StringUtil::Lower(option);
  return variable == "1" || variable == "true" || variable == "yes";
}

Optimizer::Optimizer(const Catalog &catalog, const std::unordered_map<std::string, std::string> &session_variables) : catalog_(catalog) {
    force_starter_rule_ = IsForceStarterRule(GetSessionVariable(session_variables, "force_optimizer_starter_rule"));
    vector_index_match_method_ = StringUtil::Lower(GetSessionVariable(session_variables, "vector_index_method"));
  }

auto Optimizer::Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (force_starter_rule_) {
    // Use starter rules when `force_starter_rule_` is set to true.
    auto p = plan;
    p = OptimizeMergeProjection(p);
    p = OptimizeMergeFilterNLJ(p);
    p = OptimizeNLJAsIndexJoin(p);
    p = OptimizeOrderByAsIndexScan(p);
    p = OptimizeSortLimitAsTopN(p);
    p = OptimizeAsVectorIndexScan(p);
    p = OptimizeMergeFilterScan(p);
    p = OptimizeSeqScanAsIndexScan(p);
    return p;
  }
  // By default, use user-defined rules.
  return OptimizeCustom(plan);
}

auto Optimizer::EstimatedCardinality(const std::string &table_name) -> std::optional<size_t> {
  if (StringUtil::EndsWith(table_name, "_1m")) {
    return std::make_optional(1000000);
  }
  if (StringUtil::EndsWith(table_name, "_100k")) {
    return std::make_optional(100000);
  }
  if (StringUtil::EndsWith(table_name, "_50k")) {
    return std::make_optional(50000);
  }
  if (StringUtil::EndsWith(table_name, "_10k")) {
    return std::make_optional(10000);
  }
  if (StringUtil::EndsWith(table_name, "_1k")) {
    return std::make_optional(1000);
  }
  if (StringUtil::EndsWith(table_name, "_100")) {
    return std::make_optional(100);
  }
  return std::nullopt;
}

}  // namespace bustub
