#pragma once

#include <random>
#include <unordered_map>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

struct NSW {
  using Vector = std::vector<double>;
  // reference to HNSW's vertices vector
  const std::vector<Vector> &vertices_;
  // distance function
  VectorExpressionType dist_fn_;
  // maximum number of edges of each vertex in this layer
  size_t m_max_{};
  // edges of each vertex in this layer, key is the vertex id of HNSW
  std::unordered_map<size_t, std::vector<size_t>> edges_{};
  // vertices in this layer
  std::vector<size_t> in_vertices_{};

  // search the layer and get `limit` number of approximate nearest neighbors to base_vector from the specified entry
  // points, sorted by distance
  auto SearchLayer(const std::vector<double> &base_vector, size_t limit, const std::vector<size_t> &entry_points)
      -> std::vector<size_t>;
  // insert a key into the layer, only used when implementing NSW-only index
  auto Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m);
  // add a vertex to this layer
  auto AddVertex(size_t vertex_id);
  // connect two vertices
  void Connect(size_t vertex_a, size_t vertex_b);
  // the default entry point for a layer is the first element inserted
  auto DefaultEntryPoint() -> size_t { return in_vertices_[0]; }
};

// select m nearest elements from the base vector in vertex_ids
auto SelectNeighbors(const std::vector<double> &vec, const std::vector<size_t> &vertex_ids,
                     const std::vector<std::vector<double>> &vertices, size_t m, VectorExpressionType dist_fn)
    -> std::vector<size_t>;

class HNSWIndex : public VectorIndex {
 public:
  HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
            VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options);

  ~HNSWIndex() override = default;

  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;

  auto AddVertex(const std::vector<double> &vec, RID rid) -> size_t;

  using Vector = std::vector<double>;
  std::unique_ptr<std::vector<Vector>> vertices_;
  std::vector<RID> rids_;
  std::vector<NSW> layers_;

  // number of edges to create each time a vertex is inserted
  size_t m_;
  // number of neighbors to search when inserting
  size_t ef_construction_;
  // number of neighbors to search when lookup
  size_t ef_search_;
  // maximum number of edges in all layers except layer 0
  size_t m_max_;
  // maximum number of edges in layer 0
  size_t m_max_0_;
  // random number generator
  std::mt19937 generator_;
  // level normalization factor
  double m_l_;
};

}  // namespace bustub