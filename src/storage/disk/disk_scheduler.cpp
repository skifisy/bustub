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
#include <mutex>
#include <optional>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, int thread_num)
    : disk_manager_(disk_manager), thread_num_(thread_num) {
  for (int i = 0; i < thread_num; i++) {
    background_threads_.emplace_back([=]() { StartWorkerThread(i); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (int i = 0; i < thread_num_; i++) {
    request_queue_.Put(std::nullopt);
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
  BUSTUB_ASSERT(!r.frame_ || (r.is_write_ && r.frame_->write_back_done_ == false) ||
                    (!r.is_write_ && r.frame_->write_back_done_ == true),
                "error");
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread(int thread_id) {
  // 循环获取DiskRequest
  while (std::optional<DiskRequest> request = request_queue_.Get()) {
    auto &frame = request->frame_;
    if (request->is_write_) {
      // 写数据
      disk_manager_->WritePage(request->page_id_, request->data_);
      if (frame) {
        std::lock_guard<std::mutex> lock(frame->mutex_io_);
        BUSTUB_ASSERT(!frame->write_back_done_, "error");
        BUSTUB_ASSERT(!frame->has_read_done_, "error");
        BUSTUB_ASSERT(!frame->has_read_launched_, "error");
        frame->write_back_done_ = true;
        frame->cv_.notify_all();
      } else {
        request->callback_.set_value(true);
      }
    } else {
      // 读数据
      disk_manager_->ReadPage(request->page_id_, request->data_);
      if (frame) {
        std::lock_guard<std::mutex> lock(frame->mutex_io_);
        BUSTUB_ASSERT(!frame->has_read_done_, "error");
        BUSTUB_ASSERT(frame->write_back_done_, "error");
        BUSTUB_ASSERT(frame->has_read_launched_, "error");
        frame->has_read_done_ = true;
        frame->cv_.notify_all();
      } else {
        request->callback_.set_value(true);
      }
    }
  }
}

}  // namespace bustub
