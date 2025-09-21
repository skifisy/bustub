//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, int thread_num)
    : disk_manager_(disk_manager), request_queues_(thread_num), thread_num_(thread_num) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  for (int i = 0; i < thread_num; i++) {
    background_threads_.emplace_back([=]() { StartWorkerThread(i); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (auto &request_queue : request_queues_) {
    request_queue.Put(std::nullopt);
  }
  for (auto &background_thread : background_threads_) {
    if (background_thread.has_value()) {
      background_thread->join();
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  page_id_t pid = r.page_id_;
  BUSTUB_ASSERT(pid >= 0, "page_id should be non-negative");
  request_queues_[pid % thread_num_].Put(std::move(r));
}

void DiskScheduler::StartWorkerThread(int thread_id) {
  // 循环获取DiskRequest
  while (std::optional<DiskRequest> request = request_queues_[thread_id].Get()) {
    if (request->is_write_) {
      // 写数据
      disk_manager_->WritePage(request->page_id_, request->data_);
      request->callback_.set_value(true);
    } else {
      // 读数据
      disk_manager_->ReadPage(request->page_id_, request->data_);
      request->callback_.set_value(true);
    }
  }
}

}  // namespace bustub
