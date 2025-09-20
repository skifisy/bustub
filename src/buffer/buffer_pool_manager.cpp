//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = 0;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // 通过increaseDiskSpace来确保在磁盘上有足够的空间
  page_id_t page_id = next_page_id_++;
  disk_scheduler_->IncreaseDiskSpace(page_id + 1);
  // TODO(question): 问题，申请一个page的时候，是否需要给它提供一个内存页，也就是frame？
  // AllocateFrame(page_id, false);
  return page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 从内存和磁盘中删除该page
  // 如果被pinned了，那么返回false

  // 注意所有用到page和page元信息的地方！
  std::lock_guard<std::mutex> lock_guard(*bpm_latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_it->second;
  auto frame = frames_[frame_id];
  if (frame->pin_count_ > 0) {
    return false;
  }
  // 磁盘中删除
  disk_scheduler_->DeallocatePage(page_id);
  // 从内存中删除
  // 1. replacer
  replacer_->Remove(frame_id);
  // 2. buffer_pool_manager
  page_table_.erase(page_id);
  free_frames_.emplace_back(frame_id);
  frame->Reset();
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  auto frame_id = AllocateFrame(page_id);
  if (!frame_id) {
    return std::nullopt;
  }
  return WritePageGuard(page_id, frames_[frame_id.value()], replacer_, bpm_latch_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // page data只有通过page guards访问（用于保证线程安全）
  // 用户可以通过ReadPageGuard读取页面，多个线程可以同时读取同一个page
  auto frame_id = AllocateFrame(page_id);
  if (!frame_id) {
    return std::nullopt;
  }
  return ReadPageGuard(page_id, frames_[frame_id.value()], replacer_, bpm_latch_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * @requirement 已经对bpm_latch_加锁
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];
  DiskRequest r = {true, frame->GetDataMut(), page_id, {}};

  std::future<bool> fut = r.callback_.get_future();
  disk_scheduler_->Schedule(std::move(r));
  bool ret = fut.get();
  BUSTUB_ASSERT(ret == true, "flush error!");
  // frame->is_dirty_ = false; // todo：是否要更改is_dirty_状态?
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // 所有存在于memory中的page，写入disk
  // TODO(question) 直接遍历frames_？遍历维护的page_table？
  // 是否要加ReadPageGuard?
  // 无论是否为dirty都必须要写入磁盘吗？
  std::lock_guard lock(*bpm_latch_);
  std::vector<std::pair<std::shared_ptr<FrameHeader>, std::future<bool>>> future_vec;
  future_vec.reserve(page_table_.size());
  for (auto &it : page_table_) {
    frame_id_t frame_id = it.second;
    auto &frame = frames_[frame_id];
    DiskRequest r = {true, frame->GetDataMut(), it.first, {}};
    future_vec.emplace_back(frame, r.callback_.get_future());
    disk_scheduler_->Schedule(std::move(r));
  }
  // for (auto &pair : future_vec) {
  //   pair.second.get();
  //   auto &frame = pair.first;
  //   frame->is_dirty_ = false;
  // }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::lock_guard<std::mutex> lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  frame_id_t frame_id = it->second;
  auto &frame = frames_[frame_id];
  return frame->pin_count_.load();
}

auto BufferPoolManager::AllocateFrame(page_id_t page_id, bool read_from_disk) -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(*bpm_latch_);
  auto read_page_from_disk = [this](page_id_t page_id, std::shared_ptr<FrameHeader> &frame) {
    DiskRequest r = {false, frame->GetDataMut(), page_id, {}};
    auto future = r.callback_.get_future();
    disk_scheduler_->Schedule(std::move(r));
    bool ret = future.get();  // 阻塞等待磁盘读取
    BUSTUB_ASSERT(ret, true);
  };

  // 1. 页表中是否存在该page
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    return it->second;
  }
  // 2. 存在空闲frame
  if (!free_frames_.empty()) {
    frame_id_t frame_id = free_frames_.back();
    free_frames_.pop_back();
    page_table_[page_id] = frame_id;
    if (read_from_disk) {
      read_page_from_disk(page_id, frames_[frame_id]);
    } else {
      frames_[frame_id]->Reset();           // todo: 是否可以去掉？
      frames_[frame_id]->is_dirty_ = true;  // 新建的frame要写回
    }
    frames_[frame_id]->page_id_ = page_id;
    return frame_id;
  }
  // 3. 不存在空闲frame，执行frame淘汰机制
  auto frame_id = replacer_->Evict();

  if (!frame_id) {
    return std::nullopt;
  }
  // todo: 报错，访问一个不存在的frame
  BUSTUB_ASSERT(frame_id.value() >= 0, "frame_id should bigger than 0!");
  BUSTUB_ASSERT(frame_id != -1, "frame_id should not be -1!");
  BUSTUB_ASSERT((size_t)frame_id.value() < frames_.size(), "frame_id is bigger than size!");
  auto &frame = frames_[frame_id.value()];
  page_id_t page_id_before = frame->page_id_;
  // TODO(question)  4. 处理脏页的问题
  // write-back
  FlushPage(page_id_before);

  page_table_.erase(page_id_before);
  page_table_[page_id] = frame_id.value();
  frame->Reset();
  frame->page_id_ = page_id;
  if (read_from_disk) {
    read_page_from_disk(page_id, frame);
  } else {
    frame->is_dirty_ = true;
  }
  // attention: 此处应该让replacer_认为该frame不可替换
  replacer_->SetEvictable(frame_id.value(), false);

  return frame_id.value();
}

}  // namespace bustub
