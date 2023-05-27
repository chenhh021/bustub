//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (ordered_set_.empty()) {
    return false;
  }

  *frame_id = (*ordered_set_.begin())->GetFrameId();
  ordered_set_.erase(ordered_set_.begin());
  node_store_[*frame_id]->SetEvictable(false);
  node_store_[*frame_id]->ClearHistory();
  --curr_size_;
  //  std::string size = "Left evictable nodes: " + std::to_string(curr_size_) + "/" +
  //  std::to_string(ordered_set_.size()); LOG_DEBUG("%s", size.c_str());
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  ++current_timestamp_;
  if (node_store_.count(frame_id) == 0) {
    LRUKNode node(frame_id, k_);
    auto node_ptr = std::make_shared<LRUKNode>(node);
    node_store_[frame_id] = node_ptr;
  }
  auto current_node = node_store_[frame_id];
  if (current_node->IsEvictable()) {
    ordered_set_.erase(current_node);
  }
  current_node->AddHistory(current_timestamp_);
  if (current_node->IsEvictable()) {
    ordered_set_.insert(current_node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    LRUKNode node(frame_id, k_);
    auto node_ptr = std::make_shared<LRUKNode>(node);
    node_store_[frame_id] = node_ptr;
  }
  auto current_node = node_store_[frame_id];
  current_node->SetEvictable(set_evictable);
  if (set_evictable && ordered_set_.count(current_node) == 0) {
    ++curr_size_;
    ordered_set_.insert(current_node);
  }

  if (!set_evictable && ordered_set_.count(current_node) > 0) {
    --curr_size_;
    ordered_set_.erase(current_node);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  auto current_node = node_store_[frame_id];
  if (ordered_set_.count(current_node) != 0) {
    ordered_set_.erase(current_node);
  }
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
