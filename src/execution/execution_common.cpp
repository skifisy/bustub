//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  auto &k1 = entry_a.first;
  auto &k2 = entry_b.first;
  BUSTUB_ASSERT(k1.size() == k2.size() && !k1.empty(), "error");
  for (size_t i = 0; i < order_bys_.size(); i++) {
    auto &v1 = k1[i];
    if (v1.CompareEquals(k2[i]) == CmpBool::CmpTrue) {
      continue;
    }
    if (v1.CompareLessThan(k2[i]) == CmpBool::CmpTrue) {
      switch (order_bys_[i].first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT:
          return true;
          break;
        case OrderByType::DESC:
          return false;
          break;
        default:
          BUSTUB_ASSERT(false, "error");
          break;
      }
    }
    if (v1.CompareGreaterThan(k2[i]) == CmpBool::CmpTrue) {
      switch (order_bys_[i].first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT:
          return false;
          break;
        case OrderByType::DESC:
          return true;
          break;
        default:
          BUSTUB_ASSERT(false, "error");
          break;
      }
    }
  }
  return true;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey ret;
  for (auto &order_by : order_bys) {
    ret.emplace_back(order_by.second->Evaluate(&tuple, schema));
  }
  return ret;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base
 * tuple. All logs in the undo_logs are applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction,
 * the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the
 * tuple is deleted as the result, returns std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // 如果最后的undo log是删除，那么无需重建，直接返回空值
  if (undo_logs.empty()) {
    if (base_meta.is_deleted_) {
      return std::nullopt;
    }
    return base_tuple;
  }
  if (undo_logs.back().is_deleted_) {
    return std::nullopt;
  }

  std::vector<Value> values;
  values.resize(schema->GetColumnCount());
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    values[i] = base_tuple.GetValue(schema, i);
  }

  // 对base_tuple依次应用undolog
  for (auto &undo_log : undo_logs) {
    if (undo_log.is_deleted_) {
      continue;
    }
    std::vector<Column> modified_cols;
    std::vector<size_t> idx;  // 对应原始schema的位置
    for (size_t i = 0; i < undo_log.modified_fields_.size(); i++) {
      if (undo_log.modified_fields_[i]) {
        modified_cols.emplace_back(schema->GetColumn(i));
        idx.emplace_back(i);
      }
    }
    // 构建partial schema
    Schema partial_schema(modified_cols);
    // 还原原始的values
    for (size_t i = 0; i < partial_schema.GetColumnCount(); i++) {
      size_t base_idx = idx[i];
      values[base_idx] = undo_log.tuple_.GetValue(&partial_schema, i);
    }
  }

  return Tuple(values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the
 * txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple().
 * std::nullopt if the tuple did not exist at the time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  timestamp_t read_time = txn->GetReadTs();
  timestamp_t txn_id = txn->GetTransactionTempTs();
  std::vector<UndoLog> undo_logs;

  // case 1: table heap版本就是要读的版本
  if (base_meta.ts_ <= read_time) {
    return undo_logs;
  }
  // case 3: table heap的版本恰好是当前事务修改，但是尚未提交的版本
  if (base_meta.ts_ == txn_id) {
    return undo_logs;
  }

  // case2: 需要根据版本链，找到正确的版本
  bool found = false;
  while (undo_link.has_value()) {
    auto undo_log_opt = txn_mgr->GetUndoLogOptional(*undo_link);
    if (undo_log_opt.has_value()) {
      UndoLog &undo_log = *undo_log_opt;
      undo_logs.emplace_back(undo_log);
      undo_link = undo_log.prev_version_;
      // 判断是否找到对应的版本
      if (undo_log.ts_ <= read_time || undo_log.ts_ == txn_id) {
        found = true;
        break;
      }
    } else {
      undo_link = std::nullopt;
    }
  }

  return found ? std::optional(undo_logs) : std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple
 * at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from
 * the table heap. nullptr if the tuple is deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a
 * deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // case1: Insert
  if (base_tuple == nullptr) {
    return UndoLog{true, {}, {}, ts, prev_version};
  }

  // case2: Delete
  if (target_tuple == nullptr) {
    return UndoLog{false, std::vector<bool>(schema->GetColumnCount(), true), *base_tuple, ts, prev_version};
  }

  // case3: Update
  // if(IsTupleContentEqual(*base_tuple, *target_tuple)) {
  //   return UndoLog{false, {}, {}, ts, prev_version};
  // }
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  std::vector<Value> modified_vals;
  std::vector<Column> partial_cols;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    Value origin = base_tuple->GetValue(schema, i);
    Value target = target_tuple->GetValue(schema, i);
    if (!origin.CompareExactlyEquals(target)) {
      modified_fields[i] = true;
      modified_vals.emplace_back(std::move(origin));
      partial_cols.emplace_back(schema->GetColumn(i));
    }
  }
  Schema partial_chema(partial_cols);
  return UndoLog{false, std::move(modified_fields), Tuple(modified_vals, &partial_chema), ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the
 * tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from
 * the table heap. nullptr if the tuple is deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a
 * deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  if (log.is_deleted_) {
    return log;
  }

  // case1: Insert
  if (base_tuple == nullptr && target_tuple != nullptr) {
    // 恢复到原始版本的tuple，然后generate一个undo_log
    // 原始版本的tuple就是log里面的tuple（所有列都发生了改变）
    return GenerateNewUndoLog(schema, &log.tuple_, target_tuple, log.ts_, log.prev_version_);
  }

  // case2: Delete
  if (target_tuple == nullptr) {
    // 重建，获取原始版本
    auto tuple_opt = ReconstructTuple(schema, *base_tuple, {0, false}, {log});
    BUSTUB_ASSERT(tuple_opt.has_value(), "tuple should have value");
    return UndoLog{false, std::vector<bool>(schema->GetColumnCount(), true), *tuple_opt, log.ts_, log.prev_version_};
  }
  // case3: Update
  std::vector<Column> undo_partial_cols;
  for (size_t i = 0; i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i]) {
      undo_partial_cols.emplace_back(schema->GetColumn(i));
    }
  }
  Schema undo_schema(undo_partial_cols);

  size_t idx = 0;
  std::vector<Value> modified_vals;
  std::vector<Column> partial_cols;
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);

  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    // 如果该列被修改了，从undo_log中获取原值
    // 推荐undolog只进行增量，不删除变化的值（容易发现并发bug）
    if (log.modified_fields_[i]) {
      modified_fields[i] = true;
      partial_cols.emplace_back(schema->GetColumn(i));
      modified_vals.emplace_back(log.tuple_.GetValue(&undo_schema, idx));
      idx++;
      continue;
    }
    Value origin = base_tuple->GetValue(schema, i);
    Value target = target_tuple->GetValue(schema, i);
    if (!origin.CompareExactlyEquals(target)) {
      modified_fields[i] = true;
      modified_vals.emplace_back(std::move(origin));
      partial_cols.emplace_back(schema->GetColumn(i));
    }
  }
  Schema partial_schema(partial_cols);
  return UndoLog{false, std::move(modified_fields), Tuple(modified_vals, &partial_schema), log.ts_, log.prev_version_};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(stderr,
  //              "You see this line of text because you have not implemented "
  //              "`TxnMgrDbg`. You should do this once you have "
  //              "finished task 2. Implementing this helper function will save "
  //              "you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and
  // print the version chain. An example output of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  auto to_readable_ts = [](timestamp_t time) -> std::string {
    bool is_txn = static_cast<bool>(time & TXN_START_ID);
    return is_txn ? std::string("txn") + std::to_string(time ^ TXN_START_ID) : std::to_string(time);
  };
  auto schema = &table_info->schema_;
  auto log_to_string = [&](const UndoLog &log) -> std::string {
    if (log.is_deleted_) {
      return "<del>";
    }
    std::stringstream ss;
    ss << "(";
    std::vector<Column> cols;
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        cols.emplace_back(schema->GetColumn(i));
      }
    }
    size_t idx = 0;
    Schema partial_schema(cols);
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        auto value = log.tuple_.GetValue(&partial_schema, idx);
        ss << (value.IsNull() ? "NULL" : value.ToString());
        idx++;
      } else {
        ss << "_";
      }
      if (i != log.modified_fields_.size() - 1) {
        ss << ", ";
      }
    }
    ss << ")";
    return ss.str();
  };

  // 遍历table_heap的所有tuple，打印每个tuple的所有version
  auto table_iter = table_heap->MakeIterator();
  for (; !table_iter.IsEnd(); ++table_iter) {
    RID rid = table_iter.GetRID();
    auto [meta, tuple] = table_iter.GetTuple();
    fmt::println(stderr, "RID={}/{} ts={} {}tuple={}", rid.GetPageId(), rid.GetSlotNum(), to_readable_ts(meta.ts_),
                 meta.is_deleted_ ? "<del marker> " : "", tuple.ToString(schema));

    // 处理所有版本
    for (auto undo_link = txn_mgr->GetUndoLink(rid); undo_link.has_value();) {
      // 如果undo_log_opt有值则继续，否则终止循环
      auto undo_log_opt = undo_link.has_value() ? txn_mgr->GetUndoLogOptional(*undo_link) : std::nullopt;
      if (!undo_log_opt.has_value()) {
        break;
      }

      const UndoLog &undo_log = *undo_log_opt;
      fmt::println(stderr, "\ttxn{}@{} {} ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                   log_to_string(undo_log), to_readable_ts(undo_log.ts_));
      undo_link = undo_log.prev_version_;
    }
  }
}

auto IsWriteWriteConflict(Transaction *txn, const TupleMeta *base_meta) -> bool {
  // case1: 一个事务修改另外一个尚未提交事务的数据
  if ((base_meta->ts_ & TXN_START_ID) != 0 && base_meta->ts_ != txn->GetTransactionTempTs()) {
    return true;
  }
  // case2: 一个事务想要修改已提交的数据，但它的读时间戳还是旧版本的
  if ((base_meta->ts_ & TXN_START_ID) == 0 && txn->GetReadTs() < base_meta->ts_) {
    return true;
  }
  return false;
}

auto GetTupleAtReadTs(RID rid, TableInfo *table_info, Transaction *txn, TransactionManager *txn_mgr)
    -> std::tuple<bool, Tuple> {
  auto [base_meta, base_tuple, undo_link_opt] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rid);
  auto undo_logs_opt = CollectUndoLogs(rid, base_meta, base_tuple, undo_link_opt, txn, txn_mgr);
  if (undo_logs_opt) {
    // 重建日志
    auto tuple_opt = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, *undo_logs_opt);
    if (tuple_opt.has_value()) {
      return {true, std::move(*tuple_opt)};
    }
  }
  return {false, {}};
}

auto GetPrimaryKeyIndex(Catalog *catalog, TableInfo *table_info) -> std::shared_ptr<IndexInfo> {
  for (auto index : catalog->GetTableIndexes(table_info->name_)) {
    if (index->is_primary_key_) {
      return index;
    }
  }
  return nullptr;
}

void DeleteTuple(RID r, TableInfo *table_info, Transaction *txn, TransactionManager *txn_mgr) {
  auto table_heap = table_info->table_.get();
  auto [base_meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_heap, r);
  // 1. 检查write-write冲突
  if (IsWriteWriteConflict(txn, &base_meta)) {
    txn->SetTainted();
    throw ExecutionException("write-write conflict in delete_executor");
  }
  // 2. 自我修改，更新撤销日志
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    // 更新撤销日志
    if (link.has_value()) {
      BUSTUB_ASSERT(link->prev_txn_ == txn->GetTransactionId(), "error");
      auto undo_log = txn->GetUndoLog(link->prev_log_idx_);
      auto new_log = GenerateUpdatedUndoLog(&table_info->schema_, base_meta.is_deleted_ ? nullptr : &base_tuple,
                                            nullptr, undo_log);
      txn->ModifyUndoLog(link->prev_log_idx_, new_log);
    }
    // 修改数据
    table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, r);
  } else {
    // 3. 其他情况，生成撤销日志，并链接
    UndoLink prev_link = link.has_value() ? *link : UndoLink();
    UndoLog new_log = GenerateNewUndoLog(&table_info->schema_, base_meta.is_deleted_ ? nullptr : &base_tuple, nullptr,
                                         base_meta.ts_, prev_link);
    txn->AppendUndoLog(new_log);
    txn->AppendWriteSet(table_info->oid_, r);
    UndoLink new_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum()) - 1};
    // 更新tuple_meta和undo_link
    bool success = UpdateTupleAndUndoLink(
        txn_mgr, r, new_link, table_heap, txn, {txn->GetTransactionTempTs(), true}, {},
        [txn](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
          return !IsWriteWriteConflict(txn, &meta);
        });
    if (!success) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict at running time!");
    }
  }
}

void UpdateTuple(
    RID r, Tuple &new_tuple, TableInfo *table_info, Catalog *catalog, Transaction *txn, TransactionManager *txn_mgr,
    std::function<bool(const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>)> &&check) {
  auto table_heap = table_info->table_.get();
  auto [base_meta, base_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_heap, r);

  // 1. 检查write-write冲突
  if (IsWriteWriteConflict(txn, &base_meta)) {
    txn->SetTainted();
    throw ExecutionException("write-write conflict in delete_executor");
  }
  // 2. 自我修改，更新撤销日志
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    if (link.has_value()) {
      BUSTUB_ASSERT(link->prev_txn_ == txn->GetTransactionId(), "error");
      auto undo_log = txn->GetUndoLog(link->prev_log_idx_);
      auto new_log = GenerateUpdatedUndoLog(&table_info->schema_, base_meta.is_deleted_ ? nullptr : &base_tuple,
                                            &new_tuple, undo_log);
      txn->ModifyUndoLog(link->prev_log_idx_, new_log);
    }
    // 修改table_heap中的数据
    table_heap->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, new_tuple, r);
  } else {
    // 3. 其他情况，生成撤销日志，并链接
    UndoLink prev_link = link.has_value() ? *link : UndoLink();
    UndoLog new_log = GenerateNewUndoLog(&table_info->schema_, base_meta.is_deleted_ ? nullptr : &base_tuple,
                                         &new_tuple, base_meta.ts_, prev_link);
    txn->AppendUndoLog(new_log);
    txn->AppendWriteSet(table_info->oid_, r);
    UndoLink new_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum()) - 1};
    // 更新tuple和link
    bool success = UpdateTupleAndUndoLink(
        txn_mgr, r, new_link, table_heap, txn, {txn->GetTransactionTempTs(), false}, new_tuple,
        [txn, &check](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> link) {
          // 检查：write-write conflict
          if (check != nullptr && !check(meta, tuple, rid, link)) {
            return false;
          }
          return !IsWriteWriteConflict(txn, &meta);
        });
    if (!success) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict at running time");
    }
  }
}

// new_tuple是否存在的标准：根据主键
// 如果new_tuple已经存在，则直接更新table_heap上的tuple
// 如果new_tuple还不存在，那么在table_heap上插入新的tuple
void InsertOrUpdateTuple(Tuple &new_tuple, TableInfo *table_info, Catalog *catalog, Transaction *txn,
                         TransactionManager *txn_mgr) {
  // step1: 先检查主键，如果已经存在于索引中，直接原地更新tuple
  auto table_heap = table_info->table_.get();
  auto index = GetPrimaryKeyIndex(catalog, table_info);
  Tuple index_key;
  if (index) {
    std::vector<RID> rids;
    index_key = new_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->ScanKey(index_key, &rids, txn);
    if (!rids.empty()) {
      BUSTUB_ASSERT(rids.size() == 1, "error");
      // 原地更新tuple（无需再插入主键了）
      // 检查tuple是否已经被删除了
      auto meta = table_heap->GetTupleMeta(rids[0]);
      if (!meta.is_deleted_) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict");
      }
      // 插入的场景，所以要保证tuple已经删除了才能插入
      // 两种情况：1. 自己进行的删除（还没有提交）；
      //  2.
      //  其他事务（已经提交）进行的删除（这种情况可能发生冲突，因为可能这个时候其他事务可能插入了数据，提交了，此时meta不是delete状态了）
      UpdateTuple(
          rids[0], new_tuple, table_info, catalog, txn, txn_mgr,
          [](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) { return meta.is_deleted_; });
      return;
    }
  }
  // step2: 如果不存在于索引中，插入tuple
  if (std::optional<RID> rid_inserted;
      (rid_inserted = table_heap->InsertTuple({txn->GetTransactionId(), false}, new_tuple))) {
    // 2.1 加入到事务的 write set
    txn->AppendWriteSet(table_info->oid_, *rid_inserted);
    // 2.2 插入索引
    if (index) {
      bool inserted = index->index_->InsertEntry(index_key, *rid_inserted, txn);
      if (!inserted) {
        txn->SetTainted();
        throw ExecutionException("the tuple is already exists in the primary key index");
      }
    }
  }
}

}  // namespace bustub
