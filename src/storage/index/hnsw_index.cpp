#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
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
using hnsw::VectorComparatorGreater;
using hnsw::VectorComparatorLess;

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

auto NSW::SearchLayer(const std::vector<double> &base_vector, size_t limit, const std::vector<size_t> &entry_points)
    -> std::vector<size_t> {
  // vector_id，即hnsw中的索引id
  VectorComparatorGreater comp_greater(*this, base_vector);
  VectorComparatorLess comp_less(*this, base_vector);
  std::priority_queue<size_t, std::vector<size_t>, VectorComparatorGreater> candidate(
      comp_greater);  // 待访问的候选向量，小顶堆
  std::priority_queue<size_t, std::vector<size_t>, VectorComparatorLess> w(comp_less);  // 存储结果，大顶堆
  std::unordered_set<size_t> visited;                                                   // 存储访问过的向量
  // step1: 将entry point加入C和W
  for (auto ep : entry_points) {
    candidate.emplace(ep);
    w.emplace(ep);
    if (w.size() > limit) {
      w.pop();
    }
    visited.emplace(ep);
  }
  // step2: 贪心搜索候选集，直到没有更近的向量
  while (!candidate.empty()) {
    // 弹出候选集中的最近向量
    size_t node = candidate.top();
    candidate.pop();
    // 如果候选集中最近向量比结果集中的向量还远，直接退出
    if (comp_greater(node, w.top())) {
      break;
    }
    // 搜索node的所有邻居
    for (auto neighbor : edges_[node]) {
      if (visited.find(neighbor) == visited.end()) {
        visited.emplace(neighbor);
        candidate.emplace(neighbor);
        w.emplace(neighbor);
        if (w.size() > limit) {
          w.pop();
        }
      }
    }
  }
  std::vector<size_t> result;
  while (!w.empty()) {
    result.emplace_back(w.top());
    w.pop();
  }
  std::reverse(result.begin(), result.end());
  return result;
}

auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

auto NSW::SelectNeighborsHeuristic(const std::vector<double> &vec, const std::vector<size_t> &canditate, size_t m,
                                   bool extend, bool keepPruned) -> std::vector<size_t> {
  VectorComparatorGreater comp_greater(*this, vec);
  VectorComparatorLess comp_less(*this, vec);
  std::priority_queue<size_t, std::vector<size_t>, VectorComparatorGreater> work_heap(
      comp_greater);  // 候选邻居，小顶堆
  // std::priority_queue<size_t, std::vector<size_t>, VectorComparatorLess> result(comp_less);  // 存储结果，大顶堆
  std::vector<size_t> result;
  std::unordered_set<size_t> visited;
  for (auto v : canditate) {
    work_heap.emplace(v);
    visited.emplace(v);
  }
  // 1. （可选）扩展候选集，将候选的邻居加入到候选
  if (extend) {
    for (auto v : canditate) {
      for (auto neighbor : edges_[v]) {
        if (visited.find(neighbor) != visited.end()) {
          visited.emplace(neighbor);
          work_heap.emplace(neighbor);
        }
      }
    }
  }

  // 2. 暂存被丢弃的候选点
  std::priority_queue<size_t, std::vector<size_t>, VectorComparatorGreater> w_discarded(comp_greater);

  // 3. 启发式选择邻居
  while (!work_heap.empty() && result.size() < m) {
    auto e = work_heap.top();
    work_heap.pop();

    // e到q的距离，比e到R中任何已选邻居的距离都要近，才会选中e
    bool is_closer2q = true;
    double dist_q = ComputeDistance(vec, vertices_[e], dist_fn_);
    for (auto r : result) {
      if (dist_q >= ComputeDistance(vertices_[e], vertices_[r], dist_fn_)) {
        is_closer2q = false;
        break;
      }
    }
    if (is_closer2q) {
      result.emplace_back(e);
    } else {
      w_discarded.emplace(e);
    }
  }
  // 4. 补齐连接，如果经过上面的筛选，邻居数不够M个，那么从丢弃的节点中补齐
  if (keepPruned && result.size() < m) {
    while (!w_discarded.empty() && result.size() < m) {
      result.emplace_back(w_discarded.top());
      w_discarded.pop();
    }
  }
  return result;
}

auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m,
                 std::optional<size_t> entry_point) -> std::optional<size_t> {
  AddVertex(vertex_id);
  if (in_vertices_.size() <= 1) {
    return std::nullopt;
  }
  // 1. 在当前层搜索ef_construction个最近邻作为候选
  auto candidates =
      SearchLayer(vec, ef_construction, std::vector<size_t>{entry_point ? *entry_point : DefaultEntryPoint()});

  // 2. 启发式算法搜索M个最佳邻居
  auto neightbors = SelectNeighborsHeuristic(vec, candidates, m);
  BUSTUB_ASSERT(neightbors.size() <= m, "neighbor size should be less equal than m");
  // 3. 建立双向边
  for (auto neighbor : neightbors) {
    Connect(vertex_id, neighbor);
    // 4. 检查是否超过边数上限m_max_，可能需要缩边
    if (edges_[neighbor].size() > m_max_) {
      // 重新运行启发式算法，保留最好的m_max个连接
      auto new_conn = SelectNeighborsHeuristic(vertices_[neighbor], edges_[neighbor], m_max_);
      // 重新设置邻居
      edges_[neighbor] = std::move(new_conn);
    }
  }
  return candidates.front();
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
  size_t lc = layers_.size() - 1;
  size_t cur_ep = layers_.back().DefaultEntryPoint();
  while (lc > 0) {
    auto &cur_layer = layers_[lc];
    auto candidates = cur_layer.SearchLayer(base_vector, 1, {cur_ep});
    BUSTUB_ASSERT(candidates.size() == 1, "at least one candidate");
    cur_ep = candidates.front();
    --lc;
  }
  auto vertex_ids = layers_[0].SearchLayer(base_vector, limit, {cur_ep});
  std::vector<RID> result;
  result.reserve(vertex_ids.size());
  for (const auto &id : vertex_ids) {
    result.push_back(rids_[id]);
  }
  return result;
}

void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  auto id = AddVertex(key, rid);
  size_t level = GenerateRandomLevel();

  // 特殊处理插入第一个节点的情况
  if (layers_[0].in_vertices_.empty()) {
    // 创建从level层到第0层的所有层
    for (size_t i = 0; i <= level; ++i) {
      if (i >= layers_.size()) {
        layers_.emplace_back(NSW{*vertices_, distance_fn_, m_max_});
      }
      layers_[i].AddVertex(id);
    }
    return;
  }

  size_t ep = layers_.back().DefaultEntryPoint();
  size_t max_level = layers_.size() - 1;

  // 1. 寻找level层的入口点
  size_t lc = max_level;
  for (; lc > level; lc--) {
    auto w = layers_[lc].SearchLayer(key, 1, {ep});
    BUSTUB_ASSERT(w.size() == 1, "error");
    ep = w.front();
  }

  // 2. 从第level层插入到第0层
  std::optional<size_t> ep_opt = ep;
  for (lc = std::min(max_level, level);; lc--) {
    ep_opt = layers_[lc].Insert(key, id, ef_construction_, m_, ep_opt);
    // 因为lc为size_t，需要防止下溢
    if (lc == 0) {
      break;
    }
  }
  // 3. 更新最高层
  if (level > max_level) {
    for (lc = max_level + 1; lc <= level; lc++) {
      layers_.emplace_back(NSW{*vertices_, distance_fn_, m_max_});
      layers_[lc].AddVertex(id);
    }
    BUSTUB_ASSERT(layers_.size() == level + 1, "error");
  }
}

auto HNSWIndex::GenerateRandomLevel() -> size_t {
  std::uniform_real_distribution<double> distribution(0.0, 1.0);
  double uniform_random = distribution(generator_);
  return static_cast<size_t>(std::floor(-std::log(uniform_random) * m_l_));
}

void HNSWIndex::VisualizeGraph() const {
  const int box_width = 70;

  auto print_box_line = [&](const std::string &left, const std::string &fill, const std::string &right) {
    std::cout << left;
    for (int i = 0; i < box_width - 2; ++i) {
      std::cout << fill;
    }
    std::cout << right << "\n";
  };

  auto print_box_text = [&](const std::string &text, bool align_left = true) {
    std::cout << "| ";
    if (align_left) {
      std::cout << text;
      for (size_t i = text.length(); i < box_width - 4; ++i) {
        std::cout << " ";
      }
    } else {
      int padding = (box_width - 4 - text.length()) / 2;
      for (int i = 0; i < padding; ++i) {
        std::cout << " ";
      }
      std::cout << text;
      for (size_t i = padding + text.length(); i < box_width - 4; ++i) {
        std::cout << " ";
      }
    }
    std::cout << " |\n";
  };

  // 打印标题
  print_box_line("+", "=", "+");
  print_box_text("HNSW Graph Visualization", false);
  print_box_line("+", "=", "+");
  print_box_text(fmt::format("Total vertices: {}  |  Total layers: {}", vertices_->size(), layers_.size()));
  print_box_text(fmt::format("Parameters: m={}, ef_construction={}, ef_search={}", m_, ef_construction_, ef_search_));
  print_box_line("+", "=", "+");
  std::cout << "\n";

  // 从最高层到最低层遍历
  for (int layer_idx = static_cast<int>(layers_.size()) - 1; layer_idx >= 0; --layer_idx) {
    const auto &layer = layers_[layer_idx];

    // 层标题
    print_box_line("+", "-", "+");
    print_box_text(fmt::format("Layer {} (m_max={}, vertices={})", layer_idx, layer.m_max_, layer.in_vertices_.size()),
                   false);
    print_box_line("+", "-", "+");

    if (layer.in_vertices_.empty()) {
      print_box_text("(empty layer)", false);
    } else {
      // 显示每个顶点及其连接
      for (size_t i = 0; i < layer.in_vertices_.size(); ++i) {
        size_t vertex_id = layer.in_vertices_[i];

        // 顶点信息行
        std::string vertex_info;
        if (vertex_id < rids_.size()) {
          vertex_info = fmt::format("Vertex {:3d} [Page:{:3d}, Slot:{:3d}]", vertex_id, rids_[vertex_id].GetPageId(),
                                    rids_[vertex_id].GetSlotNum());
        } else {
          vertex_info = fmt::format("Vertex {:3d}", vertex_id);
        }
        print_box_text(vertex_info);

        // 边的连接信息
        auto edge_it = layer.edges_.find(vertex_id);
        if (edge_it != layer.edges_.end() && !edge_it->second.empty()) {
          const auto &edges = edge_it->second;
          std::string edge_info = fmt::format("  +- Edges({}): ", edges.size());

          // 构建边列表
          std::string edge_list;
          for (size_t j = 0; j < edges.size(); ++j) {
            edge_list += std::to_string(edges[j]);
            if (j < edges.size() - 1) {
              edge_list += ", ";
            }
          }

          // 如果边列表太长，分行显示
          if (edge_info.length() + edge_list.length() < box_width - 8) {
            print_box_text(edge_info + edge_list);
          } else {
            print_box_text(edge_info);
            // 分行显示边
            std::string current_line = "  |  ";
            for (size_t j = 0; j < edges.size(); ++j) {
              std::string edge_str = std::to_string(edges[j]);
              if (j < edges.size() - 1) {
                edge_str += ", ";
              }

              if (current_line.length() + edge_str.length() > box_width - 8) {
                print_box_text(current_line);
                current_line = "  |  " + edge_str;
              } else {
                current_line += edge_str;
              }
            }
            if (current_line.length() > 5) {
              print_box_text(current_line);
            }
          }
        } else {
          print_box_text("  +- No edges");
        }

        // 在顶点之间添加分隔线（除了最后一个）
        if (i < layer.in_vertices_.size() - 1) {
          print_box_line("|", ".", "|");
        }
      }
    }

    print_box_line("+", "-", "+");

    // 层间连接提示
    if (layer_idx > 0) {
      std::cout << "         vvv Entry points flow down vvv\n";
    }
    std::cout << "\n";
  }

  // 统计信息
  size_t total_edges = 0;
  size_t total_vertices = 0;
  std::map<size_t, size_t> degree_distribution;

  for (const auto &layer : layers_) {
    total_vertices += layer.in_vertices_.size();
    for (const auto &[vertex_id, edges] : layer.edges_) {
      total_edges += edges.size();
      degree_distribution[edges.size()]++;
    }
  }

  print_box_line("+", "=", "+");
  print_box_text("Statistics", false);
  print_box_line("+", "=", "+");
  print_box_text(fmt::format("Total edges across all layers: {}", total_edges));
  print_box_text(fmt::format("Total vertex occurrences: {}", total_vertices));
  print_box_text(fmt::format("Average degree: {:.2f}",
                             total_vertices > 0 ? static_cast<double>(total_edges) / total_vertices : 0.0));

  if (!degree_distribution.empty()) {
    print_box_line("+", "-", "+");
    print_box_text("Degree Distribution:");
    for (const auto &[degree, count] : degree_distribution) {
      print_box_text(
          fmt::format("  Degree {:3d}: {:4d} vertices ({:.1f}%)", degree, count, 100.0 * count / total_vertices));
    }
  }

  print_box_line("+", "=", "+");
  std::cout << "\n";
}

}  // namespace bustub
