#pragma once

#include <cstddef>
#include <limits>
#include <optional>
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
  /**
   * @brief insert a key into the layer, only used when implementing NSW-only index
   * @param ef_construction 候选点数量
   * @param m 建边数量
   * @param vertex_id 插入的向量id（hnsw索引下）
   * @param vec 插入的向量本身
   * @return 下一层的entry_point
   */
  auto Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m,
              std::optional<size_t> entry_point) -> std::optional<size_t>;

  /**
   * @brief 启发式算法搜索候选邻居
   * @param canditate 候选邻居集合 efConstruct个
   * @param m 需要返回的邻居数量
   * @param extend 是否扩展候选集（数据极度聚集时可用）
   * @param keepPruned 是否保留一部分被丢弃的节点
   */
  auto SelectNeighborsHeuristic(const std::vector<double> &vec, const std::vector<size_t> &canditate, size_t m,
                                bool extend = false, bool keepPruned = true) -> std::vector<size_t>;

  // add a vertex to this layer
  auto AddVertex(size_t vertex_id);
  // connect two vertices
  void Connect(size_t vertex_a, size_t vertex_b);
  // the default entry point for a layer is the first element inserted
  auto DefaultEntryPoint() -> size_t { return in_vertices_[0]; }
};

class HNSWIndex : public VectorIndex {
 public:
  HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
            VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options);

  ~HNSWIndex() override = default;

  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;

  auto AddVertex(const std::vector<double> &vec, RID rid) -> size_t;
  void VisualizeGraph() const;

 private:
  auto GenerateRandomLevel() -> size_t;
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

namespace hnsw {
// 大顶堆
class VectorComparatorLess {
 public:
  explicit VectorComparatorLess(const NSW &nsw, const std::vector<double> &base) : nsw_(nsw), base_vector_(base) {}
  auto operator()(size_t left_idx, size_t right_idx) -> bool {
    double d1 = ComputeDistance(nsw_.vertices_[left_idx], base_vector_, nsw_.dist_fn_);
    double d2 = ComputeDistance(nsw_.vertices_[right_idx], base_vector_, nsw_.dist_fn_);
    return d1 < d2;
  }

 private:
  const NSW &nsw_;
  const std::vector<double> &base_vector_;
};

// 小顶堆
class VectorComparatorGreater {
 public:
  explicit VectorComparatorGreater(const NSW &nsw, const std::vector<double> &base) : nsw_(nsw), base_vector_(base) {}
  auto operator()(size_t left_idx, size_t right_idx) -> bool {
    double d1 = ComputeDistance(nsw_.vertices_[left_idx], base_vector_, nsw_.dist_fn_);
    double d2 = ComputeDistance(nsw_.vertices_[right_idx], base_vector_, nsw_.dist_fn_);
    return d1 > d2;
  }

 private:
  const NSW &nsw_;
  const std::vector<double> &base_vector_;
};
}  // namespace hnsw

}  // namespace bustub
