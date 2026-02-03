#pragma once

#include "common/config.h"

namespace bustub {
static constexpr page_id_t DB_META_PAGE_ID = 0;
static constexpr page_id_t TABLES_TABLE_PID = 1;
static constexpr page_id_t COLUMNS_TABLE_PID = 2;
static constexpr page_id_t INDEXES_TABLE_PID = 3;
static constexpr page_id_t INDEX_COLUMNS_TABLE_PID = 4;

static constexpr uint32_t DB_META_PAGE_MAGIC_NUMBER = 0xDEADBEEF;
static constexpr uint32_t DB_META_VERSION = 1;

// 定义数据库元信息页结构
struct DBMetaPage {
  void Init() {
    magic_number_ = DB_META_PAGE_MAGIC_NUMBER;
    version_ = DB_META_VERSION;
  }

  uint32_t magic_number_;  // 用于验证文件格式
  uint32_t version_;       // 数据库版本
  // BufferPool Related
  page_id_t next_page_id_;  // 下一个可分配的页面ID
  size_t num_frames_;       // 缓冲池的帧数
  size_t k_dist_;            // LRU-K算法中的K值
};
}  // namespace bustub