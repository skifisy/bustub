

#include "catalog/catalog.h"
namespace bustub {
auto Catalog::CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema, bool create_table_heap)
    -> std::shared_ptr<TableInfo> {
  if (table_names_.count(table_name) != 0) {
    return NULL_TABLE_INFO;
  }

  // Construct the table heap
  std::unique_ptr<TableHeap> table = nullptr;

  // When create_table_heap == false, it means that we're running binder tests (where no txn will be provided) or
  // we are running shell without buffer pool. We don't need to create TableHeap in this case.
  if (create_table_heap) {
    table = std::make_unique<TableHeap>(bpm_);
  } else {
    // Otherwise, create an empty heap only for binder tests
    table = TableHeap::CreateEmptyHeap(create_table_heap);
  }

  // Fetch the table OID for the new table
  const auto table_oid = next_table_oid_.fetch_add(1);

  // Construct the table information
  auto meta = std::make_shared<TableInfo>(schema, table_name, std::move(table), table_oid);

  // Update the internal tracking mechanisms
  tables_.emplace(table_oid, meta);
  table_names_.emplace(table_name, table_oid);
  index_names_.emplace(table_name, std::unordered_map<std::string, index_oid_t>{});

  return meta;
}

auto Catalog::GetTable(const std::string &table_name) const -> std::shared_ptr<TableInfo> {
  auto table_oid = table_names_.find(table_name);
  if (table_oid == table_names_.end()) {
    // Table not found
    return NULL_TABLE_INFO;
  }

  auto meta = tables_.find(table_oid->second);
  BUSTUB_ASSERT(meta != tables_.end(), "Broken Invariant");

  return meta->second;
}

auto Catalog::GetTable(table_oid_t table_oid) const -> std::shared_ptr<TableInfo> {
  auto meta = tables_.find(table_oid);
  if (meta == tables_.end()) {
    return NULL_TABLE_INFO;
  }

  return meta->second;
}

auto Catalog::GetIndex(const std::string &index_name, const std::string &table_name) const
    -> std::shared_ptr<IndexInfo> {
  auto table = index_names_.find(table_name);
  if (table == index_names_.end()) {
    BUSTUB_ASSERT((table_names_.find(table_name) == table_names_.end()), "Broken Invariant");
    return NULL_INDEX_INFO;
  }

  auto &table_indexes = table->second;

  auto index_meta = table_indexes.find(index_name);
  if (index_meta == table_indexes.end()) {
    return NULL_INDEX_INFO;
  }

  auto index = indexes_.find(index_meta->second);
  BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");

  return index->second;
}

auto Catalog::GetIndex(const std::string &index_name, table_oid_t table_oid) const -> std::shared_ptr<IndexInfo> {
  // Locate the table metadata for the specified table OID
  auto table_meta = tables_.find(table_oid);
  if (table_meta == tables_.end()) {
    // Table not found
    return NULL_INDEX_INFO;
  }

  return GetIndex(index_name, table_meta->second->name_);
}

auto Catalog::CreateVectorIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                                const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                                const std::string &distance_fn, const std::vector<std::pair<std::string, int>> &options,
                                IndexType index_type) -> std::shared_ptr<IndexInfo> {
  // Reject the creation request for nonexistent table
  if (table_names_.find(table_name) == table_names_.end()) {
    return NULL_INDEX_INFO;
  }

  // If the table exists, an entry for the table should already be present in index_names_
  BUSTUB_ASSERT((index_names_.find(table_name) != index_names_.end()), "Broken Invariant");

  // Determine if the requested index already exists for this table
  auto &table_indexes = index_names_.find(table_name)->second;
  if (table_indexes.find(index_name) != table_indexes.end()) {
    // The requested index already exists for this table
    return NULL_INDEX_INFO;
  }

  // Construct index metdata
  auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs, false);

  // Construct the index, take ownership of metadata
  // TODO(Kyle): We should update the API for CreateIndex
  // to allow specification of the index type itself, not
  // just the key, value, and comparator types

  // TODO(chi): support both hash index and btree index
  std::unique_ptr<VectorIndex> index;
  VectorExpressionType vty;
  if (distance_fn == "vector_ip_ops") {
    vty = VectorExpressionType::InnerProduct;
  } else if (distance_fn == "vector_l2_ops") {
    vty = VectorExpressionType::L2Dist;
  } else if (distance_fn == "vector_cosine_ops") {
    vty = VectorExpressionType::CosineSimilarity;
  } else {
    UNIMPLEMENTED("unsupported distance function");
  }
  if (index_type == IndexType::VectorHNSWIndex) {
    index = std::make_unique<HNSWIndex>(std::move(meta), bpm_, vty, options);
  } else if (index_type == IndexType::VectorIVFFlatIndex) {
    index = std::make_unique<IVFFlatIndex>(std::move(meta), bpm_, vty, options);
  } else {
    UNIMPLEMENTED("Unsupported Index Type");
  }

  // Populate the index with all tuples in table heap
  auto table_meta = GetTable(table_name);
  std::vector<std::pair<std::vector<double>, RID>> data;
  for (auto iter = table_meta->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto [meta, tuple] = iter.GetTuple();
    auto value = tuple.GetValue(&table_meta->schema_, key_attrs[0]);
    data.emplace_back(value.GetVector(), iter.GetRID());
  }
  index->BuildIndex(data);

  // Get the next OID for the new index
  const auto index_oid = next_index_oid_.fetch_add(1);

  // Construct index information; IndexInfo takes ownership of the Index itself
  auto index_info = std::make_shared<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, 0,
                                                false, index_type);

  // Update internal tracking
  indexes_.emplace(index_oid, index_info);
  table_indexes.emplace(index_name, index_oid);

  return index_info;
}

auto Catalog::GetIndex(index_oid_t index_oid) -> std::shared_ptr<IndexInfo> {
  auto index = indexes_.find(index_oid);
  if (index == indexes_.end()) {
    return NULL_INDEX_INFO;
  }

  return index->second;
}

auto Catalog::GetTableIndexes(const std::string &table_name) const -> std::vector<std::shared_ptr<IndexInfo>> {
  // Ensure the table exists
  if (table_names_.find(table_name) == table_names_.end()) {
    return std::vector<std::shared_ptr<IndexInfo>>{};
  }

  auto table_indexes = index_names_.find(table_name);
  BUSTUB_ASSERT((table_indexes != index_names_.end()), "Broken Invariant");

  std::vector<std::shared_ptr<IndexInfo>> indexes{};
  indexes.reserve(table_indexes->second.size());
  for (const auto &index_meta : table_indexes->second) {
    auto index = indexes_.find(index_meta.second);
    BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");
    indexes.push_back(index->second);
  }

  return indexes;
}

auto Catalog::GetTableNames() -> std::vector<std::string> {
  std::vector<std::string> result;
  for (const auto &x : table_names_) {
    result.push_back(x.first);
  }
  return result;
}
}  // namespace bustub