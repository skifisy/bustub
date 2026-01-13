#pragma once

#include "buffer/buffer_pool_manager.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "type/value.h"

namespace bustub {

class VectorIndex : public Index {
 public:
  VectorIndex(std::unique_ptr<IndexMetadata> &&metadata, VectorExpressionType distance_fn)
      : Index(std::move(metadata)), distance_fn_(distance_fn) {}
  virtual void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) = 0;
  virtual auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> = 0;
  virtual void InsertVectorEntry(const std::vector<double> &key, RID rid) = 0;

  auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool override {
    UNIMPLEMENTED("call InsertVectorEntry instead");
  }

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override {
    UNIMPLEMENTED("delete from vector index is not supported");
  }

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override {
    UNIMPLEMENTED("call ScanVectorKey instead");
  }

  VectorExpressionType distance_fn_;
};

}  // namespace bustub
