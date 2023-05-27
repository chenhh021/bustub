//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  //  std::string debug_msg = "Init BPM with pool_size:" + std::to_string(pool_size);
  //  LOG_DEBUG("%s", debug_msg.c_str());

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  page_latches_ = new std::mutex[pool_size];

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] page_latches_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  *page_id = AllocatePage();

  //    std::string debug_msg = "NewPage:" + std::to_string(*page_id);
  //    LOG_DEBUG("%s", debug_msg.c_str());

  frame_id_t frame_id;

  if (!free_list_.empty()) {
    //    LOG_DEBUG("From free list");

    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[*page_id] = frame_id;
    Page *physical_page = &pages_[frame_id];

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
    latch_.unlock();

    physical_page->page_id_ = *page_id;
    physical_page->pin_count_ = 1;

    return physical_page;
  }

  if (replacer_->Evict(&frame_id)) {
    //    LOG_DEBUG("From evict");
    Page *evicted_page = &pages_[frame_id];

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    page_table_[*page_id] = frame_id;
    page_table_.erase(evicted_page->GetPageId());

    std::lock_guard<std::mutex> guard(page_latches_[frame_id]);

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page->GetPageId(), evicted_page->GetData());
    }
    latch_.unlock();

    evicted_page->ResetMemory();
    evicted_page->is_dirty_ = false;
    evicted_page->page_id_ = *page_id;
    evicted_page->pin_count_ = 1;

    return &pages_[frame_id];
  }

  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  //    std::string debug_msg = "FetchPage:" + std::to_string(page_id);
  //    LOG_DEBUG("%s", debug_msg.c_str());

  frame_id_t frame_id;

  latch_.lock();
  // search for page_id in the buffer pool
  if (page_table_.count(page_id) != 0) {
    //    LOG_DEBUG("In Buffer");
    frame_id = page_table_[page_id];
    Page *physical_page = &pages_[frame_id];
    if (physical_page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }

    std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
    latch_.unlock();

    ++(physical_page->pin_count_);

    return physical_page;
  }

  if (!free_list_.empty()) {
    //    LOG_DEBUG("From free list");
    frame_id = free_list_.front();
    free_list_.pop_front();

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    page_table_[page_id] = frame_id;
    Page *physical_page = &pages_[frame_id];

    std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
    latch_.unlock();

    physical_page->pin_count_ = 1;
    physical_page->page_id_ = page_id;

    disk_manager_->ReadPage(page_id, physical_page->data_);

    return physical_page;
  }

  if (replacer_->Evict(&frame_id)) {
    //    LOG_DEBUG("From evict");
    Page *evicted_page = &pages_[frame_id];

    page_table_[page_id] = frame_id;
    page_table_.erase(evicted_page->GetPageId());

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    std::lock_guard<std::mutex> guard(page_latches_[frame_id]);

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page->GetPageId(), evicted_page->GetData());
    }
    latch_.unlock();

    evicted_page->is_dirty_ = false;
    evicted_page->page_id_ = page_id;
    evicted_page->pin_count_ = 1;

    disk_manager_->ReadPage(page_id, evicted_page->GetData());

    return &pages_[frame_id];
  }

  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();

  //  std::string str_dirty = is_dirty ? " true" : " false";
  //  std::string debug_msg = "UnpinPage:" + std::to_string(page_id) + str_dirty;
  //  LOG_DEBUG("%s", debug_msg.c_str());

  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }

  auto frame_id = page_table_[page_id];
  auto physical_page = &pages_[frame_id];

  std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
  if (physical_page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }

  if (--(physical_page->pin_count_) == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch_.unlock();

  if (!physical_page->is_dirty_) {
    physical_page->is_dirty_ = is_dirty;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();

  //  std::string debug_msg = "FlushPage:" + std::to_string(page_id);
  //  LOG_DEBUG("%s", debug_msg.c_str());

  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }

  auto frame_id = page_table_[page_id];
  auto physical_page = &pages_[frame_id];
  std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
  latch_.unlock();

  physical_page->is_dirty_ = false;

  disk_manager_->WritePage(page_id, physical_page->GetData());

  return true;
}

// unchecked
void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  //  LOG_DEBUG("FlushAllPage");

  for (auto &page_mapping : page_table_) {
    auto page_id = page_mapping.first;
    auto frame_id = page_mapping.second;
    auto physical_page = &pages_[frame_id];

    page_latches_[frame_id].lock();
    physical_page->is_dirty_ = false;
    disk_manager_->WritePage(page_id, physical_page->GetData());
    page_latches_[frame_id].unlock();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();

  //  std::string debug_msg = "DeletePage:" + std::to_string(page_id);
  //  LOG_DEBUG("%s", debug_msg.c_str());

  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return true;
  }

  auto frame_id = page_table_[page_id];
  auto physical_page = &pages_[frame_id];

  if (physical_page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  std::lock_guard<std::mutex> guard(page_latches_[frame_id]);
  latch_.unlock();

  physical_page->is_dirty_ = false;
  physical_page->ResetMemory();
  physical_page->page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto physical_page = FetchPage(page_id);
  physical_page->RLatch();
  return {this, physical_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  //  std::string log_info = "Thread "+std::to_string(pthread_self())+":acquire write guard for page_id: " +
  //  std::to_string(page_id); LOG_DEBUG("%s", log_info.c_str());
  auto physical_page = FetchPage(page_id);
  physical_page->WLatch();
  return {this, physical_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
