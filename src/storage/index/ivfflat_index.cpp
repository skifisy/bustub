#include "storage/index/ivfflat_index.h"
#include <algorithm>
#include <limits>
#include <optional>
#include <queue>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
using Vector = std::vector<double>;

IVFFlatIndex::IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                           VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn) {
  std::optional<size_t> lists;
  std::optional<size_t> probe_lists;
  for (const auto &[key, value] : options) {
    if (key == "lists") {
      lists = value;
    } else if (key == "probe_lists") {
      probe_lists = value;
    }
  }
  if (!lists.has_value() || !probe_lists.has_value()) {
    throw Exception("missing options: lists / probe_lists for ivfflat index");
  }
  lists_ = *lists;
  probe_lists_ = *probe_lists;
}

void VectorAdd(Vector &vec_a, const Vector &vec_b) {
  for (size_t i = 0; i < vec_a.size(); i++) {
    vec_a[i] += vec_b[i];
  }
}

void VectorScalarDiv(Vector &vec, double scalar) {
  for (auto &element : vec) {
    element /= scalar;
  }
}

// Find the nearest centroid to the base vector in all centroids
auto FindCentroid(const Vector &vec, const std::vector<Vector> &centroids, VectorExpressionType dist_fn) -> size_t {
  double min_distance = std::numeric_limits<double>::max();
  size_t min_centroid_idx = 0;

  for (size_t i = 0; i < centroids.size(); i++) {
    double current_distance = ComputeDistance(vec, centroids[i], dist_fn);
    if (current_distance < min_distance) {
      min_distance = current_distance;
      min_centroid_idx = i;
    }
  }
  return min_centroid_idx;
}

// Compute new centroids based on the original centroids using K-Means
auto FindCentroids(const std::vector<std::pair<Vector, RID>> &data, const std::vector<Vector> &centroids,
                   VectorExpressionType dist_fn) -> std::vector<Vector> {
  // Assign each vector to its nearest centroid
  std::vector<std::vector<Vector>> centroid_buckets(centroids.size());

  for (const auto &[vec, rid] : data) {
    auto centroid_idx = FindCentroid(vec, centroids, dist_fn);
    centroid_buckets[centroid_idx].emplace_back(vec);
  }

  // Calculate new centroids as the mean of assigned vectors
  std::vector<Vector> new_centroids;
  size_t dimension = centroids[0].size();

  for (const auto &bucket : centroid_buckets) {
    Vector centroid_sum(dimension, 0.0);
    for (const auto &vec : bucket) {
      VectorAdd(centroid_sum, vec);
    }
    if (!bucket.empty()) {
      VectorScalarDiv(centroid_sum, static_cast<double>(bucket.size()));
    }
    new_centroids.emplace_back(std::move(centroid_sum));
  }

  return new_centroids;
}

void IVFFlatIndex::BuildIndex(std::vector<std::pair<Vector, RID>> initial_data) {
  if (initial_data.empty()) {
    return;
  }

  // Initialize centroids by randomly selecting from data points
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dist(0, initial_data.size() - 1);

  centroids_.clear();
  centroids_.reserve(lists_);

  std::unordered_set<size_t> selected_indices;
  while (selected_indices.size() < lists_ && selected_indices.size() < initial_data.size()) {
    size_t idx = dist(gen);
    if (selected_indices.insert(idx).second) {
      centroids_.emplace_back(initial_data[idx].first);
    }
  }

  // K-Means iterations to optimize centroids
  constexpr int k_max_k_means_iterations = 500;
  for (int iter = 0; iter < k_max_k_means_iterations; iter++) {
    centroids_ = FindCentroids(initial_data, centroids_, distance_fn_);
  }

  // Assign data points to their nearest centroids
  centroids_buckets_.clear();
  centroids_buckets_.resize(lists_);

  for (const auto &[vec, rid] : initial_data) {
    auto centroid_idx = FindCentroid(vec, centroids_, distance_fn_);
    centroids_buckets_[centroid_idx].emplace_back(vec, rid);
  }
}

void IVFFlatIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto centroid_idx = FindCentroid(key, centroids_, distance_fn_);
  centroids_buckets_[centroid_idx].emplace_back(key, rid);
}

// Find top-k nearest vectors in a bucket using max-heap
auto IVFFlatIndex::FindTopKInBucket(std::vector<std::pair<Vector, RID>> &bucket, VectorComparatorLess &comparator,
                                    size_t limit) -> std::vector<std::pair<Vector, RID>> {
  using HeapType =
      std::priority_queue<std::pair<Vector, RID>, std::vector<std::pair<Vector, RID>>, VectorComparatorLess>;
  HeapType max_heap(comparator);

  // Build initial heap with first 'limit' elements
  size_t idx = 0;
  for (size_t i = 0; i < limit && idx < bucket.size(); i++, idx++) {
    max_heap.emplace(bucket[idx]);
  }

  // Maintain heap by replacing elements with smaller distances
  for (; idx < bucket.size(); idx++) {
    if (comparator(bucket[idx], max_heap.top())) {
      max_heap.pop();
      max_heap.emplace(bucket[idx]);
    }
  }

  // Extract results in reverse order (smallest to largest distance)
  std::vector<std::pair<Vector, RID>> top_k_results;
  top_k_results.reserve(max_heap.size());
  while (!max_heap.empty()) {
    top_k_results.emplace_back(max_heap.top());
    max_heap.pop();
  }
  std::reverse(top_k_results.begin(), top_k_results.end());

  return top_k_results;
}

// Merge multiple sorted buckets and return top-k results using k-way merge
auto IVFFlatIndex::MergeSortedBuckets(std::vector<std::vector<std::pair<Vector, RID>>> &sorted_buckets,
                                      VectorComparatorLess &comparator, size_t limit) -> std::vector<RID> {
  std::vector<RID> result;
  result.reserve(limit);

  // Node in the min-heap for k-way merge
  struct HeapNode {
    std::pair<Vector, RID> data_;
    size_t bucket_idx_;  // Source bucket index
    size_t pos_;         // Position in the source bucket

    HeapNode(std::pair<Vector, RID> d, size_t b, size_t p) : data_(std::move(d)), bucket_idx_(b), pos_(p) {}
  };

  // Min-heap comparator (reverse of VectorComparator for priority_queue)
  auto heap_comparator = [&comparator](const HeapNode &lhs, const HeapNode &rhs) {
    return !comparator(lhs.data_, rhs.data_);
  };

  std::priority_queue<HeapNode, std::vector<HeapNode>, decltype(heap_comparator)> min_heap(heap_comparator);

  // Initialize heap with the first element from each bucket
  for (size_t bucket_idx = 0; bucket_idx < sorted_buckets.size(); bucket_idx++) {
    if (!sorted_buckets[bucket_idx].empty()) {
      min_heap.emplace(sorted_buckets[bucket_idx][0], bucket_idx, 0);
    }
  }

  // Extract top-k elements using k-way merge
  while (!min_heap.empty() && result.size() < limit) {
    auto node = min_heap.top();
    min_heap.pop();

    result.emplace_back(node.data_.second);

    // Add next element from the same bucket to the heap
    size_t next_pos = node.pos_ + 1;
    if (next_pos < sorted_buckets[node.bucket_idx_].size()) {
      min_heap.emplace(sorted_buckets[node.bucket_idx_][next_pos], node.bucket_idx_, next_pos);
    }
  }

  return result;
}

auto IVFFlatIndex::GetProbeCentroids(const std::vector<double> &base_vector) -> std::vector<size_t> {
  VectorComparatorLess comparator(base_vector, distance_fn_);

  // Use max-heap to find probe_lists_ nearest centroids
  using CentroidHeap =
      std::priority_queue<std::pair<Vector, size_t>, std::vector<std::pair<Vector, size_t>>, VectorComparatorLess>;
  CentroidHeap max_heap(comparator);

  // Build initial heap
  size_t idx = 0;
  for (size_t i = 0; i < probe_lists_ && idx < centroids_.size(); i++, idx++) {
    max_heap.emplace(centroids_[idx], idx);
  }

  // Update heap with nearer centroids
  for (; idx < centroids_.size(); idx++) {
    if (comparator(std::make_pair(centroids_[idx], idx), max_heap.top())) {
      max_heap.pop();
      max_heap.emplace(centroids_[idx], idx);
    }
  }

  // Extract centroid indices
  std::vector<size_t> probe_centroid_indices;
  probe_centroid_indices.reserve(max_heap.size());
  while (!max_heap.empty()) {
    probe_centroid_indices.emplace_back(max_heap.top().second);
    max_heap.pop();
  }

  return probe_centroid_indices;
}

auto IVFFlatIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  // Step 1: Find nearest probe_lists_ centroids
  auto probe_centroid_indices = GetProbeCentroids(base_vector);

  // Step 2: Find top-k vectors in each probed bucket
  VectorComparatorLess comparator(base_vector, distance_fn_);
  std::vector<std::vector<std::pair<Vector, RID>>> sorted_buckets;
  sorted_buckets.reserve(probe_centroid_indices.size());

  for (auto centroid_idx : probe_centroid_indices) {
    sorted_buckets.emplace_back(FindTopKInBucket(centroids_buckets_[centroid_idx], comparator, limit));
  }

  // Step 3: Merge sorted buckets and return final top-k results
  return MergeSortedBuckets(sorted_buckets, comparator, limit);
}

}  // namespace bustub