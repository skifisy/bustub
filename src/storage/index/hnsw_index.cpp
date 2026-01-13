#include <algorithm>
#include <cmath>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "fmt/format.h"
#include "fmt/std.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
HNSWIndex::HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                     VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn),
      vertices_(std::make_unique<std::vector<Vector>>()),
      layers_{{*vertices_, distance_fn}} {
  std::optional<size_t> m;
  std::optional<size_t> ef_construction;
  std::optional<size_t> ef_search;
  for (const auto &[k, v] : options) {
    if (k == "m") {
      m = v;
    } else if (k == "ef_construction") {
      ef_construction = v;
    } else if (k == "ef_search") {
      ef_search = v;
    }
  }
  if (!m.has_value() || !ef_construction.has_value() || !ef_search.has_value()) {
    throw Exception("missing options: m / ef_construction / ef_search for hnsw index");
  }
  ef_construction_ = *ef_construction;
  m_ = *m;
  ef_search_ = *ef_search;
  m_max_ = m_;
  m_max_0_ = m_ * m_;
  layers_[0].m_max_ = m_max_0_;
  m_l_ = 1.0 / std::log(m_);
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
}

auto SelectNeighbors(const std::vector<double> &vec, const std::vector<size_t> &vertex_ids,
                     const std::vector<std::vector<double>> &vertices, size_t m, VectorExpressionType dist_fn)
    -> std::vector<size_t> {
  return {};
}

auto NSW::SearchLayer(const std::vector<double> &base_vector, size_t limit, const std::vector<size_t> &entry_points)
    -> std::vector<size_t> {
  return {};
}

auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m) {
  // IMPLEMENT ME
  AddVertex(vertex_id);
}

void NSW::Connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

auto HNSWIndex::AddVertex(const std::vector<double> &vec, RID rid) -> size_t {
  auto id = vertices_->size();
  vertices_->emplace_back(vec);
  rids_.emplace_back(rid);
  return id;
}

void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {
  std::shuffle(initial_data.begin(), initial_data.end(), generator_);

  for (const auto &[vec, rid] : initial_data) {
    InsertVectorEntry(vec, rid);
  }
}

auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  auto vertex_ids = layers_[0].SearchLayer(base_vector, limit, {layers_[0].DefaultEntryPoint()});
  std::vector<RID> result;
  result.reserve(vertex_ids.size());
  for (const auto &id : vertex_ids) {
    result.push_back(rids_[id]);
  }
  return result;
}

void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto id = AddVertex(key, rid);
  layers_[0].Insert(key, id, ef_construction_, m_);
}

}  // namespace bustub