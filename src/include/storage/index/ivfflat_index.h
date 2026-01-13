
#pragma once

#include <memory>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

class IVFFlatIndex : public VectorIndex {
 public:
  IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
               VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options);

  ~IVFFlatIndex() override = default;

  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;
  auto GetProbeCentroids(const std::vector<double> &base_vector) -> std::vector<size_t>;

  BufferPoolManager *bpm_;
  // number of buckets or lists to create when building the index
  size_t lists_{0};
  // number of buckets or lists to probe when lookup
  size_t probe_lists_{0};

  using Vector = std::vector<double>;

  // vector of each centroid
  std::vector<Vector> centroids_;

  // vectors and RIDs in each of the centroid list
  std::vector<std::vector<std::pair<Vector, RID>>> centroids_buckets_;

 private:
  class VectorComparatorLess {
   public:
    explicit VectorComparatorLess(Vector base, VectorExpressionType dist_fn)
        : base_vector_(std::move(base)), dist_fn_(dist_fn) {}

    auto operator()(const std::pair<Vector, RID> &lhs, const std::pair<Vector, RID> &rhs) const -> bool {
      double dist_lhs = ComputeDistance(lhs.first, base_vector_, dist_fn_);
      double dist_rhs = ComputeDistance(rhs.first, base_vector_, dist_fn_);
      return dist_lhs < dist_rhs;
    }

    auto operator()(const std::pair<Vector, size_t> &lhs, const std::pair<Vector, size_t> &rhs) const -> bool {
      double dist_lhs = ComputeDistance(lhs.first, base_vector_, dist_fn_);
      double dist_rhs = ComputeDistance(rhs.first, base_vector_, dist_fn_);
      return dist_lhs < dist_rhs;
    }

   private:
    Vector base_vector_;
    VectorExpressionType dist_fn_;
  };
  auto FindTopKInBucket(std::vector<std::pair<Vector, RID>> &bucket, VectorComparatorLess &comparator, size_t limit)
      -> std::vector<std::pair<Vector, RID>>;
  auto MergeSortedBuckets(std::vector<std::vector<std::pair<Vector, RID>>> &sorted_buckets,
                          VectorComparatorLess &comparator, size_t limit) -> std::vector<RID>;
};

}  // namespace bustub
