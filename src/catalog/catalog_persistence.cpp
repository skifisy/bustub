#include "catalog/catalog_persistence.h"
#include <cstdint>
#include <memory>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/db_meta_page.h"
#include "storage/table/table_heap.h"

namespace bustub {
auto CatalogPersistence::InitializeSystemTables(Transaction *txn) -> bool {
  // 创建系统表
  tables_table_ = catalog_->CreateTable(txn, SYSTEM_TABLES, CreateTablesTableSchema());
  columns_table_ = catalog_->CreateTable(txn, SYSTEM_COLUMNS, CreateColumnsTableSchema());
  PersistTable(txn, tables_table_);
  PersistTable(txn, columns_table_);
  indexes_table_ = catalog_->CreateTable(txn, SYSTEM_INDEXES, CreateIndexesTableSchema());
  index_columns_table_ = catalog_->CreateTable(txn, SYSTEM_INDEX_COLUMNS, CreateIndexColumnsTableSchema());
  BUSTUB_ASSERT(tables_table_->table_->GetFirstPageId() == TABLES_TABLE_PID, "tables_table_ oid not match");
  BUSTUB_ASSERT(columns_table_->table_->GetFirstPageId() == COLUMNS_TABLE_PID, "columns_table_ oid not match");
  BUSTUB_ASSERT(indexes_table_->table_->GetFirstPageId() == INDEXES_TABLE_PID, "indexes_table_ oid not match");
  BUSTUB_ASSERT(index_columns_table_->table_->GetFirstPageId() == INDEX_COLUMNS_TABLE_PID,
                "index_columns_table_ oid not match");

  return tables_table_ != nullptr && columns_table_ != nullptr && indexes_table_ != nullptr &&
         index_columns_table_ != nullptr;
}

auto CatalogPersistence::LoadCatalog(Transaction *txn) -> bool {
  // 构建系统表 TableInfo（使用已知的固定 PageId 和 Schema）
  // 后续使用 MakeEagerIterator 加载数据
  {
    auto heap = std::make_unique<TableHeap>(catalog_->bpm_, TABLES_TABLE_PID, INVALID_PAGE_ID);
    auto schema = CreateTablesTableSchema();
    tables_table_ = std::make_shared<TableInfo>(schema, SYSTEM_TABLES, std::move(heap), TABLES_TABLE_PID);
  }
  {
    auto heap = std::make_unique<TableHeap>(catalog_->bpm_, COLUMNS_TABLE_PID, INVALID_PAGE_ID);
    auto schema = CreateColumnsTableSchema();
    columns_table_ = std::make_shared<TableInfo>(schema, SYSTEM_COLUMNS, std::move(heap), COLUMNS_TABLE_PID);
  }
  {
    auto heap = std::make_unique<TableHeap>(catalog_->bpm_, INDEXES_TABLE_PID, INVALID_PAGE_ID);
    auto schema = CreateIndexesTableSchema();
    indexes_table_ = std::make_shared<TableInfo>(schema, SYSTEM_INDEXES, std::move(heap), INDEXES_TABLE_PID);
  }
  {
    auto heap = std::make_unique<TableHeap>(catalog_->bpm_, INDEX_COLUMNS_TABLE_PID, INVALID_PAGE_ID);
    auto schema = CreateIndexColumnsTableSchema();
    index_columns_table_ =
        std::make_shared<TableInfo>(schema, SYSTEM_INDEX_COLUMNS, std::move(heap), INDEX_COLUMNS_TABLE_PID);
  }
  if (!LoadTables(txn)) {
    return false;
  }
  tables_table_ = catalog_->GetTable(SYSTEM_TABLES);
  columns_table_ = catalog_->GetTable(SYSTEM_COLUMNS);
  indexes_table_ = catalog_->GetTable(SYSTEM_INDEXES);
  index_columns_table_ = catalog_->GetTable(SYSTEM_INDEX_COLUMNS);
  return true;
}

auto CatalogPersistence::CreateTablesTableSchema() -> Schema {
  std::vector<Column> columns = {Column{"table_oid", TypeId::INTEGER}, Column{"table_name", TypeId::VARCHAR, 128},
                                 Column{"first_page_id", TypeId::INTEGER}, Column{"last_page_id", TypeId::INTEGER},
                                 Column{"column_count", TypeId::INTEGER}};
  return Schema{columns};
}

auto CatalogPersistence::CreateColumnsTableSchema() -> Schema {
  std::vector<Column> columns = {Column{"table_oid", TypeId::INTEGER},        Column{"column_idx", TypeId::INTEGER},
                                 Column{"column_name", TypeId::VARCHAR, 128}, Column{"type_id", TypeId::INTEGER},
                                 Column{"storage_size", TypeId::INTEGER},     Column{"column_offset", TypeId::INTEGER}};
  return Schema{columns};
}

auto CatalogPersistence::CreateIndexesTableSchema() -> Schema {
  std::vector<Column> columns = {
      Column{"index_oid", TypeId::INTEGER},  Column{"index_name", TypeId::VARCHAR, 128},
      Column{"table_oid", TypeId::INTEGER},  Column{"table_name", TypeId::VARCHAR, 128},
      Column{"key_size", TypeId::INTEGER},   Column{"is_primary_key", TypeId::INTEGER},  // 0 或 1
      Column{"index_type", TypeId::INTEGER}, Column{"root_page_id", TypeId::INTEGER}};
  return Schema{columns};
}

auto CatalogPersistence::CreateIndexColumnsTableSchema() -> Schema {
  std::vector<Column> columns = {
      Column{"index_oid", TypeId::INTEGER},        Column{"key_idx", TypeId::INTEGER},
      Column{"table_column_idx", TypeId::INTEGER}, Column{"column_name", TypeId::VARCHAR, 128},
      Column{"type_id", TypeId::INTEGER},          Column{"type_size", TypeId::INTEGER},
      Column{"variable_length", TypeId::INTEGER},  Column{"column_offset", TypeId::INTEGER}};
  return Schema{columns};
}

auto CatalogPersistence::PersistTable(Transaction *txn, const std::shared_ptr<TableInfo> &table_info) -> bool {
  if (tables_table_ == nullptr || columns_table_ == nullptr) {
    // 系统表未初始化
    return false;
  }
  // 1. 插入表的基本信息到 __catalog_tables
  page_id_t first_page_id = table_info->table_->GetFirstPageId();

  std::vector<Value> table_values = {
      Value{TypeId::INTEGER, static_cast<int32_t>(table_info->oid_)}, Value{TypeId::VARCHAR, table_info->name_},
      Value{TypeId::INTEGER, static_cast<int32_t>(first_page_id)},
      Value{TypeId::INTEGER, static_cast<int32_t>(table_info->table_->GetLastPageId())},
      Value{TypeId::INTEGER, static_cast<int32_t>(table_info->schema_.GetColumnCount())}};

  Tuple table_tuple{table_values, &tables_table_->schema_};
  auto rid = tables_table_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, table_tuple, nullptr, txn);

  if (!rid.has_value()) {
    return false;
  }

  // 2. 插入所有列信息到 __catalog_columns
  return PersistColumns(txn, table_info->oid_, table_info->schema_);
}

auto CatalogPersistence::PersistColumns(Transaction *txn, table_oid_t table_oid, const Schema &schema) -> bool {
  const auto &columns = schema.GetColumns();

  for (uint32_t i = 0; i < columns.size(); i++) {
    const auto &col = columns[i];

    std::vector<Value> values = {Value{TypeId::INTEGER, static_cast<int32_t>(table_oid)},
                                 Value{TypeId::INTEGER, static_cast<int32_t>(i)},
                                 Value{TypeId::VARCHAR, col.GetName()},
                                 Value{TypeId::INTEGER, static_cast<int32_t>(col.GetType())},
                                 Value{TypeId::INTEGER, static_cast<int32_t>(col.GetStorageSize())},
                                 Value{TypeId::INTEGER, static_cast<int32_t>(col.GetOffset())}};

    Tuple tuple{values, &columns_table_->schema_};
    auto rid = columns_table_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, tuple, nullptr, txn);

    if (!rid.has_value()) {
      return false;
    }
  }

  return true;
}

auto CatalogPersistence::LoadTables(Transaction *txn) -> bool {
  auto iter = tables_table_->table_->MakeEagerIterator();

  while (!iter.IsEnd()) {
    auto [meta, tuple] = iter.GetTuple();
    if (!meta.is_deleted_) {
      // 解析表的基本信息
      table_oid_t table_oid = tuple.GetValue(&tables_table_->schema_, 0).GetAs<int32_t>();
      auto table_name = tuple.GetValue(&tables_table_->schema_, 1).ToString();
      auto first_page_id = tuple.GetValue(&tables_table_->schema_, 2).GetAs<int32_t>();
      auto last_page_id = tuple.GetValue(&tables_table_->schema_, 3).GetAs<int32_t>();
      auto column_count = tuple.GetValue(&tables_table_->schema_, 4).GetAs<int32_t>();
      // 重建 TableHeap
      auto table_heap = std::make_unique<TableHeap>(catalog_->bpm_, first_page_id, last_page_id);

      // 创建 TableInfo
      auto table_info = std::make_shared<TableInfo>(Schema({}), table_name, std::move(table_heap), table_oid);

      // 临时设置Schema为空
      auto &columns = table_columns_[table_oid];
      columns.resize(column_count);

      // 注册到 Catalog
      catalog_->tables_[table_oid] = table_info;
      catalog_->table_names_[table_name] = table_oid;

      // 更新 next_table_oid_
      if (table_oid >= catalog_->next_table_oid_) {
        catalog_->next_table_oid_ = table_oid + 1;
      }
    }
    ++iter;
  }
  LoadColumnsForTable(txn);
  return true;
}

void CatalogPersistence::LoadColumnsForTable(Transaction *txn) {
  auto iter = columns_table_->table_->MakeEagerIterator();

  while (!iter.IsEnd()) {
    auto [meta, tuple] = iter.GetTuple();

    if (!meta.is_deleted_) {
      table_oid_t tid = tuple.GetValue(&columns_table_->schema_, 0).GetAs<int32_t>();
      uint32_t column_idx = tuple.GetValue(&columns_table_->schema_, 1).GetAs<int32_t>();
      auto column_name = tuple.GetValue(&columns_table_->schema_, 2).ToString();
      TypeId type_id = static_cast<TypeId>(tuple.GetValue(&columns_table_->schema_, 3).GetAs<int32_t>());
      uint32_t storage_size = tuple.GetValue(&columns_table_->schema_, 4).GetAs<int32_t>();
      uint32_t column_offset = tuple.GetValue(&columns_table_->schema_, 5).GetAs<int32_t>();
      // 重建 Column
      Column col(column_name, type_id, storage_size, column_offset);
      BUSTUB_ASSERT(table_columns_[tid].size() > column_idx, "column index overflow!");
      table_columns_[tid][column_idx] = std::move(col);
    }
    ++iter;
  }
  // 重构schema
  for (auto &[table_oid, columns_map] : table_columns_) {
    Schema schema{columns_map};
    auto table_info = catalog_->tables_[table_oid];
    table_info->schema_ = std::move(schema);
  }
}

auto CatalogPersistence::PersistIndex(Transaction *txn, const std::shared_ptr<IndexInfo> &index_info) -> bool {
  // 1. 插入索引基本信息
  page_id_t root_page_id = INVALID_PAGE_ID;  // 需要从具体索引获取

  std::vector<Value> index_values = {
      Value{TypeId::INTEGER, static_cast<int32_t>(index_info->index_oid_)},
      Value{TypeId::VARCHAR, index_info->name_},
      Value{TypeId::INTEGER, static_cast<int32_t>(catalog_->GetTable(index_info->table_name_)->oid_)},  // 需要查找
      Value{TypeId::VARCHAR, index_info->table_name_},
      Value{TypeId::INTEGER, static_cast<int32_t>(index_info->key_size_)},
      Value{TypeId::INTEGER, index_info->is_primary_key_ ? 1 : 0},
      Value{TypeId::INTEGER, static_cast<int32_t>(index_info->index_type_)},
      Value{TypeId::INTEGER, static_cast<int32_t>(root_page_id)}};

  Tuple index_tuple{index_values, &indexes_table_->schema_};
  auto rid = indexes_table_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, index_tuple, nullptr, txn);

  if (!rid.has_value()) {
    return false;
  }

  // 2. 插入索引列信息
  // 需要从 IndexInfo 中获取 key_attrs
  std::vector<uint32_t> key_attrs;  // 需要添加到 IndexInfo 或从 IndexMetadata 获取

  return PersistIndexColumns(txn, index_info->index_oid_, index_info->key_schema_, key_attrs);
}

auto CatalogPersistence::PersistIndexColumns(Transaction *txn, index_oid_t index_oid, const Schema &key_schema,
                                             const std::vector<uint32_t> &key_attrs) -> bool {
  return false;
}
}  // namespace bustub