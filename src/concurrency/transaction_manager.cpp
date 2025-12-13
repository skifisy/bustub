//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/watermark.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // 1. 设置txn的read timestamp
  txn_ref->read_ts_ = last_commit_ts_.load();
  // 2. 更新watermark
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // 获取提交时间戳
  txn->commit_ts_ = ++last_commit_ts_;

  // TODO(fall2023): Implement the commit logic!
  // 将事务中待写回的数据写回
  std::scoped_lock<std::mutex> txn_lock(txn->latch_);
  for (auto &[oid, rid_set] : txn->write_set_) {
    auto table_info = catalog_->GetTable(oid);
    auto table_heap = table_info->table_.get();
    for (auto rid : rid_set) {
      auto [meta, tuple, first_undo_link] = GetTupleAndUndoLink(this, table_heap, rid);
      BUSTUB_ASSERT(meta.ts_ == txn->GetTransactionTempTs(), "base tuple's ts should equal to txn's temp ts");
      meta.ts_ = txn->GetCommitTs();
      auto ret = UpdateTupleAndUndoLink(this, rid, first_undo_link, table_heap, txn, meta, tuple);
      BUSTUB_ASSERT(ret, "update fail!");
    }
  }

  // mvcc保证同时只有一个未提交的transaction能修改base_tuple （同时的修改会abort掉）
  // 所以，不会生成一个未提交 && 修改数据的版本链
  // for (auto &undo_log : txn->undo_logs_) {
  //   undo_log.ts_ = txn->commit_ts_;
  // }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t water_mark = running_txns_.GetWatermark();
  std::unordered_map<table_oid_t, std::unordered_set<RID>> modified_rids;
  std::unordered_set<txn_id_t> txn_can_delete;
  // step1: 遍历write_set寻找修改过的table_id + rid
  for (auto [txn_id, txn] : txn_map_) {
    // 只需要看已提交事务的tuple
    if (txn->GetTransactionState() == TransactionState::COMMITTED) {
      txn_can_delete.insert(txn_id);
      for (auto &[table_id, rid_set] : txn->write_set_) {
        auto it = modified_rids.find(table_id);
        if (it != modified_rids.end()) {
          modified_rids[table_id].insert(rid_set.begin(), rid_set.end());
        } else {
          modified_rids[table_id] = rid_set;
        }
      }
    }
  }

  // step2: 遍历每个table中对应的tuple，构建不可删除的事务集合
  for (auto &[table_id, rids] : modified_rids) {
    auto table = catalog_->GetTable(table_id);
    for (auto rid : rids) {
      auto [meta, tuple, link_opt] = GetTupleAndUndoLink(this, table->table_.get(), rid);
      // 从meta 到 第一个 ts<=water_mark 日志都需要保留，后面的日志需要删除
      if (meta.ts_ <= water_mark) {
        continue;
      }

      UndoLink link = link_opt.has_value() ? *link_opt : UndoLink();
      while (link.IsValid()) {
        auto log = GetUndoLogOptional(link);
        if (!log) {
          break;
        }
        txn_can_delete.erase(link_opt->prev_txn_);  // 该log需要保留
        if (log->ts_ <= water_mark) {
          break;  // 后续的log都无需保留
        }
        link = log->prev_version_;
      }
    }
  }

  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
  // step3: 遍历所有transaction
  for (auto txn_id : txn_can_delete) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
