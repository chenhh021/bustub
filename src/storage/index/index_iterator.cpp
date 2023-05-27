/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard *r_guard, int idx, page_id_t page_id,
                                  int cur_size)
    : bpm_(bpm), page_id_(page_id), cur_idx_(idx), cur_size_(cur_size) {
  if (r_guard == nullptr) {
    r_guard_ = std::nullopt;
  } else {
    r_guard_.emplace(std::move(*r_guard));
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return r_guard_ == std::nullopt; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  //  std::string loginfo = "Thread "+std::to_string(pthread_self())+":Iterator operator *";
  //  LOG_DEBUG("%s", loginfo.c_str());
  BUSTUB_ENSURE(!IsEnd(), "Reached the end");
  if (leaf_ == nullptr) {
    leaf_ = r_guard_->As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
  return leaf_->PairAt(cur_idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  //  std::string loginfo = "Thread "+std::to_string(pthread_self())+":Iterator operator ++";
  //  LOG_DEBUG("%s", loginfo.c_str());
  BUSTUB_ENSURE(!IsEnd(), "Reached the end");
  if (++cur_idx_ == cur_size_) {
    if (leaf_ == nullptr) {
      leaf_ = r_guard_->As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    }
    page_id_ = leaf_->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      r_guard_ = std::nullopt;
    } else {
      auto next_guard = bpm_->FetchPageRead(page_id_);
      r_guard_.emplace(std::move(next_guard));
      leaf_ = r_guard_->As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      cur_size_ = leaf_->GetSize();
    }
    cur_idx_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
