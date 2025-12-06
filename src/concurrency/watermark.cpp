#include <cstddef>
#include <exception>

#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/watermark.h"
#include "storage/table/tuple.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if (read_ts < watermark_ || watermark_ == INVALID_TS) {
    watermark_ = read_ts; 
  }
  auto it = current_reads_.find(read_ts);
  if (it != current_reads_.end()) {
    it->second++;
  } else {
    current_reads_[read_ts] = 1;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto it = current_reads_.find(read_ts);
  BUSTUB_ASSERT(it != current_reads_.end(), "read ts not exists");
  it->second--;
  if (it->second == 0) {
    current_reads_.erase(it);
    if (read_ts == watermark_) {
      // 更新水位
      if (current_reads_.empty()) {
        watermark_ = INVALID_TS;
      } else {
        watermark_ = current_reads_.begin()->first;
      }
    }
  }
}

}  // namespace bustub
