//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard *r_guard, int idx, page_id_t page_id, int cur_size);
  ~IndexIterator();  // NOLINT

  IndexIterator(const IndexIterator &) = delete;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;
  IndexIterator(IndexIterator &&that) noexcept;
  auto operator=(IndexIterator &&that) noexcept -> IndexIterator &;

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (page_id_ == INVALID_PAGE_ID && itr.page_id_ == INVALID_PAGE_ID) ||
           (page_id_ == itr.page_id_ && cur_idx_ == itr.cur_idx_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  std::optional<ReadPageGuard> r_guard_{std::nullopt};
  page_id_t page_id_;
  size_t cur_idx_;
  size_t cur_size_;
  const B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_ = nullptr;
};

}  // namespace bustub
