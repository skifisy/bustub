#pragma once

#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/vector_index_scan_plan.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/vector_index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

class VectorIndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */

  VectorIndexScanExecutor(ExecutorContext *exec_ctx, const VectorIndexScanPlanNode *plan);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The index scan plan node to be executed. */
  const VectorIndexScanPlanNode *plan_;
  VectorIndex *index_;
  TableHeap *table_;
  std::vector<RID> result_;
  size_t cnt_{0};
};

}  // namespace bustub
