#pragma once

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** ComparisonType represents the type of comparison that we want to perform. */
enum class VectorExpressionType { L2Dist, InnerProduct, CosineSimilarity };

inline auto ComputeL2Distance(const std::vector<double> &left, const std::vector<double> &right) -> double {
  double sum = 0;
  size_t len = left.size();
  for (size_t i = 0; i < len; ++i) {
    sum = sum + (left[i] - right[i]) * (left[i] - right[i]);
  }
  return std::sqrt(sum);
}

inline auto ComputeInnerProduct(const std::vector<double> &left, const std::vector<double> &right) -> double {
  double ret = 0;
  for (size_t i = 0; i < left.size(); i++) {
    ret -= left[i] * right[i];
  }
  return ret;
}

inline auto ComputeCosineSimilarity(const std::vector<double> &left, const std::vector<double> &right) -> double {
  double s1 = 0;
  double s2 = 0;
  double s3 = 0;
  size_t len = left.size();
  for (size_t i = 0; i < len; i++) {
    double a = left[i];
    double b = right[i];
    s1 += a * b;
    s2 += a * a;
    s3 += b * b;
  }

  // 使用一个极小值 epsilon 来判断是否为 0
  constexpr double epsilon = 1e-9;
  if (s2 < epsilon || s3 < epsilon) {
    // 当其中一个向量为零向量时，余弦相似度无定义。
    // 视业务逻辑而定，通常返回 1.0 (表示距离最大，不相似) 或者 0.0
    return 1.0;
  }

  // 计算余弦相似度 (Cosine Similarity)
  double cos_sim = s1 / (std::sqrt(s2 * s3));
  return 1.0 - cos_sim;
}

inline auto ComputeDistance(const std::vector<double> &left, const std::vector<double> &right,
                            VectorExpressionType dist_fn) {
  auto sz = left.size();
  BUSTUB_ASSERT(sz == right.size(), "vector length mismatched!");
  switch (dist_fn) {
    case VectorExpressionType::L2Dist: {
      return ComputeL2Distance(left, right);
    }
    case VectorExpressionType::InnerProduct: {
      return ComputeInnerProduct(left, right);
    }
    case VectorExpressionType::CosineSimilarity: {
      return ComputeCosineSimilarity(left, right);
    }
    default:
      BUSTUB_ASSERT(false, "Unsupported vector expr type.");
  }
}

class VectorExpression : public AbstractExpression {
 public:
  VectorExpression(VectorExpressionType expr_type, AbstractExpressionRef left, AbstractExpressionRef right)
      : AbstractExpression({std::move(left), std::move(right)}, Column{"<val>", TypeId::DECIMAL}),
        expr_type_{expr_type} {}

  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema);
    Value rhs = GetChildAt(1)->Evaluate(tuple, schema);
    return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  /** @return the string representation of the expression node and its children
   */
  auto ToString() const -> std::string override {
    return fmt::format("{}({}, {})", expr_type_, *GetChildAt(0), *GetChildAt(1));
  }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(VectorExpression);

  VectorExpressionType expr_type_;

 private:
  auto PerformComputation(const Value &lhs, const Value &rhs) const -> double {
    auto left_vec = lhs.GetVector();
    auto right_vec = rhs.GetVector();
    return ComputeDistance(left_vec, right_vec, expr_type_);
  }
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::VectorExpressionType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::VectorExpressionType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case bustub::VectorExpressionType::L2Dist:
        name = "l2_dist";
        break;
      case bustub::VectorExpressionType::CosineSimilarity:
        name = "cosine_similarity";
        break;
      case bustub::VectorExpressionType::InnerProduct:
        name = "inner_product";
        break;
    default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
