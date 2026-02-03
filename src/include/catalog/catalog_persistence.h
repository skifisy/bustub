#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "concurrency/transaction.h"

namespace bustub {

class CatalogPersistence {
 public:
  explicit CatalogPersistence(Catalog *catalog) : catalog_(catalog) {}

  /**
   * 初始化所有系统表
   */
  auto InitializeSystemTables(Transaction *txn = nullptr) -> bool;

  /**
   * 从磁盘加载整个 Catalog
   */
  auto LoadCatalog(Transaction *txn = nullptr) -> bool;

  /**
   * 持久化表及其 Schema
   */
  auto PersistTable(Transaction *txn, const std::shared_ptr<TableInfo> &table_info) -> bool;

  /**
   * 持久化索引及其 Key Schema
   */
  auto PersistIndex(Transaction *txn, const std::shared_ptr<IndexInfo> &index_info) -> bool;

  /**
   * 删除表及其所有列信息
   */
  auto RemoveTable(Transaction *txn, table_oid_t table_oid) -> bool;

  /**
   * 删除索引及其所有列信息
   */
  auto RemoveIndex(Transaction *txn, index_oid_t index_oid) -> bool;

 private:
  Catalog *catalog_;

  // 系统表名称
  static constexpr const char *SYSTEM_TABLES = "__catalog_tables";
  static constexpr const char *SYSTEM_COLUMNS = "__catalog_columns";
  static constexpr const char *SYSTEM_INDEXES = "__catalog_indexes";
  static constexpr const char *SYSTEM_INDEX_COLUMNS = "__catalog_index_columns";

  // 创建系统表 Schema
  auto CreateTablesTableSchema() -> Schema;
  auto CreateColumnsTableSchema() -> Schema;
  auto CreateIndexesTableSchema() -> Schema;
  auto CreateIndexColumnsTableSchema() -> Schema;

  // 持久化辅助函数
  auto PersistColumns(Transaction *txn, table_oid_t table_oid, const Schema &schema) -> bool;
  auto PersistIndexColumns(Transaction *txn, index_oid_t index_oid, const Schema &key_schema,
                           const std::vector<uint32_t> &key_attrs) -> bool;

  // 加载辅助函数
  auto LoadTables(Transaction *txn) -> bool;
  auto LoadIndexes(Transaction *txn) -> bool;
  void LoadColumnsForTable(Transaction *txn);
  auto LoadColumnsForIndex(Transaction *txn, index_oid_t index_oid) -> std::tuple<Schema, std::vector<uint32_t>>;

  // 工具函数
  auto ColumnToValues(const Column &col, table_oid_t table_oid, uint32_t column_idx) -> std::vector<Value>;
  auto ValuesToColumn(const std::vector<Value> &values) -> Column;

  // 系统表缓存
  std::shared_ptr<TableInfo> tables_table_;
  std::shared_ptr<TableInfo> columns_table_;
  std::shared_ptr<TableInfo> indexes_table_;
  std::shared_ptr<TableInfo> index_columns_table_;

  //
  std::unordered_map<table_oid_t, std::vector<Column>> table_columns_;
};

}  // namespace bustub