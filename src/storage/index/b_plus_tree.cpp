#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) +
                        ":Initialize tree with leaf size: " + std::to_string(leaf_max_size) +
                        " and internal size:" + std::to_string(internal_max_size);
  LOG_DEBUG("%s", loginfo.c_str());
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  std::string loginfo =
      "Thread " + std::to_string(pthread_self()) + ":Search for key: " + std::to_string(key.ToString());
  LOG_DEBUG("%s", loginfo.c_str());
  // Declaration of context instance.
  if (IsEmpty()) {
    return false;
  }
  // Get root page
  std::optional<ReadPageGuard> guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard->As<BPlusTreeHeaderPage>();
  int root_page_id = root_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(root_page_id);
  guard = std::nullopt;  // release head

  auto node = node_guard.As<BPlusTreePage>();
  while (!node->IsLeafPage()) {
    auto inner_node = node_guard.As<InternalPage>();
    int next_id = inner_node->KeyIndex(key, comparator_);
    page_id_t next_page_id = inner_node->ValueAt(next_id);
    node_guard = bpm_->FetchPageRead(next_page_id);
    node = node_guard.As<BPlusTreePage>();
  }

  auto leaf = node_guard.As<LeafPage>();
  ValueType tmp_result;
  if (leaf->GetValue(key, &tmp_result, comparator_)) {
    result->push_back(tmp_result);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":Insert key: " + std::to_string(key.ToString());
  LOG_DEBUG("%s", loginfo.c_str());
  // if empty, just insert a new leaf page
  if (IsEmpty()) {
    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);

    page_id_t root_page_id;

    auto new_root_page = reinterpret_cast<LeafPage *>(bpm_->NewPage(&root_page_id)->GetData());

    loginfo = "Thread " + std::to_string(pthread_self()) + ":New leaf page with id " + std::to_string(root_page_id);
    LOG_DEBUG("%s", loginfo.c_str());

    new_root_page->Init(leaf_max_size_);
    new_root_page->InsertValue(key, value, comparator_);
    auto header = header_guard.AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = root_page_id;
    return true;
  }

  // Declaration of context instance.
  Context ctx;
  //  (void)ctx;
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  ctx.header_page_.emplace(std::move(guard));

  page_id_t root_page_id;

  std::vector<int> inner_ids;
  auto header = ctx.header_page_->As<BPlusTreeHeaderPage>();
  root_page_id = header->root_page_id_;

  ctx.write_set_.push_back(bpm_->FetchPageWrite(root_page_id));
  auto cur_page = ctx.write_set_.back().As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto inner_page = ctx.write_set_.back().As<InternalPage>();
    int next_id = inner_page->KeyIndex(key, comparator_);
    inner_ids.push_back(next_id);
    int next_page_id = inner_page->ValueAt(next_id);
    guard = bpm_->FetchPageWrite(next_page_id);
    cur_page = guard.As<BPlusTreePage>();
    ctx.write_set_.emplace_back(std::move(guard));
  }

  auto detect_page = ctx.write_set_.back().As<LeafPage>();
  ValueType dummy_value;
  if (detect_page->GetValue(key, &dummy_value, comparator_)) {
    return false;  // The key already exist
  }

  // count which pages need modification
  int modification_count = 1;
  // whether leaf page need split
  bool need_split = cur_page->GetSize() + 1 == cur_page->GetMaxSize();

  // internal pages
  for (int i = ctx.write_set_.size() - 2; i >= 0 && need_split; --i) {
    ++modification_count;
    auto page = ctx.write_set_[i].As<BPlusTreePage>();
    need_split = page->GetSize() == page->GetMaxSize();
  }

  bool root_change_flag = need_split && (modification_count == static_cast<int>(ctx.write_set_.size()));

  if (!root_change_flag) {
    ctx.header_page_ = std::nullopt;
  }

  int release_count = ctx.write_set_.size() - modification_count;
  while (release_count > 0) {
    ctx.write_set_.pop_front();
    --release_count;
  }

  // handle leaf page
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  leaf_page->InsertValue(key, value, comparator_);
  KeyType next_insert_key;
  page_id_t next_insert_value;
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    int max_size = leaf_page->GetMaxSize();
    int split_id = max_size / 2;
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_leaf_page = reinterpret_cast<LeafPage *>(bpm_->NewPage(&new_page_id)->GetData());

    loginfo = "Thread " + std::to_string(pthread_self()) + ":New leaf page with id " + std::to_string(new_page_id);
    LOG_DEBUG("%s", loginfo.c_str());

    new_leaf_page->Init(leaf_max_size_);
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_page_id);
    for (int i = split_id; i < max_size; ++i) {
      new_leaf_page->InsertAtBack(leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    leaf_page->ReduceToHalf();
    next_insert_key = new_leaf_page->KeyAt(0);
    next_insert_value = new_page_id;
    bpm_->UnpinPage(new_page_id, true);
  }
  ctx.write_set_.pop_back();

  // handle internal page
  while (!ctx.write_set_.empty()) {
    auto inner_page = ctx.write_set_.back().AsMut<InternalPage>();
    KeyType insert_key = next_insert_key;
    page_id_t insert_value = next_insert_value;
    // split internal page
    if (inner_page->GetSize() == inner_page->GetMaxSize()) {
      int to_insert_id = inner_page->KeyIndex(insert_key, comparator_) + 1;
      int max_size = inner_page->GetMaxSize();
      int split_id = (max_size + 1) / 2;  // first id on the higher half
      bool insert_to_lower = false;       // new pair insert to lower half
      bool insert_is_lift = true;         // new key are lifted

      int right_first_insert_id = split_id;  // just initialize
      KeyType lift_key = next_insert_key;    // just initialize
      auto lift_value = next_insert_value;
      if (to_insert_id < split_id) {
        lift_key = inner_page->KeyAt(split_id - 1);
        lift_value = inner_page->ValueAt(split_id - 1);
        //        right_first_insert_id == split_id; //useless as already set the same value
        insert_is_lift = false;
        insert_to_lower = true;
      } else if (to_insert_id > split_id) {
        right_first_insert_id = split_id + 1;
        lift_key = inner_page->KeyAt(split_id);
        lift_value = inner_page->ValueAt(split_id);
        insert_is_lift = false;
      }

      page_id_t new_page_id = INVALID_PAGE_ID;
      auto new_inner_page = reinterpret_cast<InternalPage *>(bpm_->NewPage(&new_page_id)->GetData());

      loginfo =
          "Thread " + std::to_string(pthread_self()) + ":New internal page with id " + std::to_string(new_page_id);
      LOG_DEBUG("%s", loginfo.c_str());

      new_inner_page->Init(internal_max_size_);
      new_inner_page->SetKeyAt(0, lift_key);  // for remove operation
      new_inner_page->SetValueAt(0, lift_value);
      for (int i = right_first_insert_id; i < max_size; ++i) {
        new_inner_page->InsertAtBack(inner_page->KeyAt(i), inner_page->ValueAt(i));
      }
      inner_page->ReduceToHalf(insert_to_lower);
      if (!insert_is_lift) {
        if (insert_to_lower) {
          inner_page->InsertValue(insert_key, insert_value, comparator_);
        } else {
          new_inner_page->InsertValue(insert_key, insert_value, comparator_);
        }
      }

      bpm_->UnpinPage(new_page_id, true);
      next_insert_key = lift_key;
      next_insert_value = new_page_id;
    } else {
      inner_page->InsertValue(insert_key, insert_value, comparator_);
    }
    ctx.write_set_.pop_back();
  }

  // root is splited
  if (root_change_flag) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page = reinterpret_cast<InternalPage *>(bpm_->NewPage(&new_page_id)->GetData());

    loginfo = "Thread " + std::to_string(pthread_self()) + ":New internal page with id " + std::to_string(new_page_id);
    LOG_DEBUG("%s", loginfo.c_str());

    new_page->Init(internal_max_size_);
    auto head_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    page_id_t old_root_page_id = head_page->root_page_id_;
    head_page->root_page_id_ = new_page_id;
    new_page->SetValueAt(
        0, old_root_page_id);  // key head is invalid hear. As it always at leftmost, it doesn't affect merge operation.
    new_page->InsertAtBack(next_insert_key, next_insert_value);
    bpm_->UnpinPage(new_page_id, true);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":Remove key: " + std::to_string(key.ToString());
  LOG_DEBUG("%s", loginfo.c_str());
  if (IsEmpty()) {
    return;
  }
  // Declaration of context instance.
  Context ctx;

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  ctx.header_page_.emplace(std::move(guard));

  page_id_t root_page_id;

  std::deque<int> inner_ids;
  std::deque<page_id_t> page_ids;
  auto header = ctx.header_page_->As<BPlusTreeHeaderPage>();
  root_page_id = header->root_page_id_;
  page_ids.push_back(root_page_id);

  ctx.write_set_.push_back(bpm_->FetchPageWrite(root_page_id));
  auto cur_page = ctx.write_set_.back().As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto inner_page = ctx.write_set_.back().As<InternalPage>();
    int next_id = inner_page->KeyIndex(key, comparator_);
    inner_ids.push_back(next_id);
    int next_page_id = inner_page->ValueAt(next_id);
    page_ids.push_back(next_page_id);
    guard = bpm_->FetchPageWrite(next_page_id);
    cur_page = guard.As<BPlusTreePage>();
    ctx.write_set_.emplace_back(std::move(guard));
  }
  auto leaf_page = ctx.write_set_.back().As<LeafPage>();
  int next_id = leaf_page->IndexAt(key, comparator_);
  if (next_id == leaf_page->GetSize() || comparator_(leaf_page->KeyAt(next_id), key) != 0) {
    return;
  }
  inner_ids.push_back(next_id);

  //  // count which pages need modification (abandoned, wrong logic)
  //  int modification_count = 1;
  //  bool root_change_flag = true;
  //  if(ctx.write_set_.size() > 1) {
  //    // whether leaf page need redistribution/merge
  //    bool need_modify = cur_page->GetSize() - 1 < cur_page->GetMinSize();
  //
  //    // internal pages
  //    for (int i = ctx.write_set_.size() - 2; i >= 1 && need_modify; --i) {
  //      ++modification_count;
  //      auto page = ctx.write_set_[i].As<BPlusTreePage>();
  //      need_modify = page->GetSize() - 1 < page->GetMinSize();
  //    }
  //
  //    // special determination for root page
  //    if (need_modify) {
  //      ++modification_count;
  //      auto page = ctx.write_set_[0].As<BPlusTreePage>();
  //      need_modify = page->GetSize() <= 2;
  //    }
  //    root_change_flag = need_modify;
  //  }else{
  //    auto page = ctx.write_set_[0].As<BPlusTreePage>();
  //    root_change_flag = page->GetSize() == 1;
  //  }

  bool root_change_flag = true;
  // only exclude impossible case
  if (!ctx.write_set_.empty()) {
    auto page = ctx.write_set_[0].As<BPlusTreePage>();
    root_change_flag = page->GetSize() < 3;
  }

  if (!root_change_flag) {
    ctx.header_page_ = std::nullopt;
  }

  //   (wrong logic)
  //  int release_count = ctx.write_set_.size() - modification_count;
  //  while (release_count > 0) {
  //    ctx.write_set_.pop_front();
  //    inner_ids.pop_front();
  //    --release_count;
  //  }

  Remove(key, ctx, inner_ids, page_ids, root_page_id, txn);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Context &ctx, std::deque<int> &idxes, std::deque<page_id_t> &page_ids,
                            page_id_t root_page_id, Transaction *txn) {
  assert(ctx.write_set_.size() == idxes.size());
  auto cur_page_read = ctx.write_set_.back().As<BPlusTreePage>();
  bool is_leaf = cur_page_read->IsLeafPage();
  bool is_root = page_ids.back() == root_page_id;
  page_id_t cur_page_id = page_ids.back();
  int depth = ctx.write_set_.size();
  if (is_leaf) {
    auto leaf = ctx.write_set_.back().template AsMut<LeafPage>();
    leaf->RemovePairAt(idxes.back());
    if (is_root) {
      if (leaf->GetSize() == 0) {
        auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
        header->root_page_id_ = INVALID_PAGE_ID;
        bpm_->DeletePage(root_page_id);
      }
    } else {
      int cur_size = leaf->GetSize();
      if (cur_size < leaf->GetMinSize()) {
        auto parent_page = ctx.write_set_[depth - 2].AsMut<InternalPage>();  // need modification anyway
        auto cur_page = ctx.write_set_.back().AsMut<LeafPage>();
        int child_id = idxes[depth - 2];

        // has previous sibling
        if (child_id > 0) {
          page_id_t pre_page_id = parent_page->ValueAt(child_id - 1);
          auto pre_page_guard = bpm_->FetchPageWrite(pre_page_id);
          auto pre_page = pre_page_guard.template AsMut<LeafPage>();
          if (pre_page->GetSize() < pre_page->GetMinSize() + 1) {
            // merge and delete
            for (int i = 0; i < cur_size; ++i) {
              pre_page->InsertAtBack(cur_page->PairAt(i));
            }

            // special for leaf page, change next_page_id
            pre_page->SetNextPageId(cur_page->GetNextPageId());

            idxes.pop_back();
            page_ids.pop_back();
            ctx.write_set_.pop_back();
            Remove(key, ctx, idxes, page_ids, root_page_id, txn);
          } else {
            KeyType new_key = Redistribute(pre_page, cur_page, true);
            parent_page->SetKeyAt(child_id, new_key);
          }
          return;
        }

        // has next sibling
        if (child_id < parent_page->GetSize() - 1) {
          page_id_t next_page_id = parent_page->ValueAt(child_id + 1);
          auto next_page_guard = bpm_->FetchPageWrite(next_page_id);
          auto next_page = next_page_guard.template AsMut<LeafPage>();
          int next_size = next_page->GetSize();
          if (next_size < next_page->GetMinSize() + 1) {
            // merge and delete
            for (int i = 0; i < next_size; ++i) {
              cur_page->InsertAtBack(next_page->PairAt(i));
            }

            // special for leaf page, change next_page_id
            cur_page->SetNextPageId(next_page->GetNextPageId());

            idxes.pop_back();
            page_ids.pop_back();
            ctx.write_set_.pop_back();

            ++idxes.back();

            Remove(key, ctx, idxes, page_ids, root_page_id, txn);
          } else {
            KeyType new_key = Redistribute(cur_page, next_page, false);
            parent_page->SetKeyAt(child_id + 1, new_key);
          }
        }
      }
    }
  } else {
    auto internal = ctx.write_set_.back().template AsMut<InternalPage>();
    internal->RemovePairAt(idxes.back());
    if (is_root) {
      if (internal->GetSize() == 1) {
        auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
        header->root_page_id_ = internal->ValueAt(0);
        bpm_->DeletePage(cur_page_id);
        return;
      }
    } else {
      int cur_size = internal->GetSize();
      if (cur_size < internal->GetMinSize()) {
        auto parent_page = ctx.write_set_[depth - 2].AsMut<InternalPage>();  // need modification anyway
        auto *cur_page = ctx.write_set_.back().AsMut<InternalPage>();
        int child_id = idxes[depth - 2];

        // has previous sibling
        if (child_id > 0) {
          page_id_t pre_page_id = parent_page->ValueAt(child_id - 1);
          auto pre_page_guard = bpm_->FetchPageWrite(pre_page_id);
          auto *pre_page = pre_page_guard.template AsMut<InternalPage>();
          if (pre_page->GetSize() < pre_page->GetMinSize() + 1) {
            // merge and delete
            for (int i = 0; i < cur_size; ++i) {
              pre_page->InsertAtBack(cur_page->PairAt(i));
            }
            idxes.pop_back();
            page_ids.pop_back();
            ctx.write_set_.pop_back();
            Remove(key, ctx, idxes, page_ids, root_page_id, txn);
          } else {
            KeyType new_key = Redistribute(pre_page, cur_page, true);
            parent_page->SetKeyAt(child_id, new_key);
          }
          return;
        }

        // has next sibling
        if (child_id < parent_page->GetSize() - 1) {
          page_id_t next_page_id = parent_page->ValueAt(child_id + 1);
          auto next_page_guard = bpm_->FetchPageWrite(next_page_id);
          auto next_page = next_page_guard.template AsMut<InternalPage>();
          int next_size = next_page->GetSize();
          if (next_size < next_page->GetMinSize() + 1) {
            // merge and delete
            for (int i = 0; i < next_size; ++i) {
              cur_page->InsertAtBack(next_page->PairAt(i));
            }
            idxes.pop_back();
            page_ids.pop_back();
            ctx.write_set_.pop_back();

            ++idxes.back();

            Remove(key, ctx, idxes, page_ids, root_page_id, txn);
          } else {
            KeyType new_key = Redistribute(cur_page, next_page, false);
            parent_page->SetKeyAt(child_id + 1, new_key);
          }
        }
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":Initiate iterator begin()";
  LOG_DEBUG("%s", loginfo.c_str());
  // Declaration of context instance.
  Context ctx;
  // Get root page
  std::optional<ReadPageGuard> guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard->As<BPlusTreeHeaderPage>();
  int root_page_id = root_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(root_page_id);
  guard = std::nullopt;  // release head

  auto node = node_guard.As<BPlusTreePage>();
  while (!node->IsLeafPage()) {
    auto inner_node = node_guard.As<InternalPage>();
    page_id_t next_page_id = inner_node->ValueAt(0);
    node_guard = bpm_->FetchPageRead(next_page_id);
    node = node_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, &node_guard, 0, node_guard.PageId(), node->GetSize());
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::string loginfo =
      "Thread " + std::to_string(pthread_self()) + ":Initiate iterator with key: " + std::to_string(key.ToString());
  LOG_DEBUG("%s", loginfo.c_str());
  // Get root page
  std::optional<ReadPageGuard> guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard->As<BPlusTreeHeaderPage>();
  int root_page_id = root_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(root_page_id);
  guard = std::nullopt;  // release head

  auto node = node_guard.As<BPlusTreePage>();
  while (!node->IsLeafPage()) {
    auto inner_node = node_guard.As<InternalPage>();
    int next_id = inner_node->KeyIndex(key, comparator_);
    page_id_t next_page_id = inner_node->ValueAt(next_id);
    node_guard = bpm_->FetchPageRead(next_page_id);
    node = node_guard.As<BPlusTreePage>();
  }
  auto leaf = node_guard.As<LeafPage>();
  int idx = leaf->IndexAt(key, comparator_);
  return INDEXITERATOR_TYPE(bpm_, &node_guard, idx, node_guard.PageId(), leaf->GetSize());
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, nullptr, 0, INVALID_PAGE_ID, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLeafPageRead(const KeyType &key, Transaction *txn) -> const ReadPageGuard & {
//   // Get root page
//   std::optional<ReadPageGuard> guard = bpm_->FetchPageRead(header_page_id_);
//   auto root_page = guard->As<BPlusTreeHeaderPage>();
//   int root_page_id = root_page->root_page_id_;
//   ReadPageGuard node_guard = bpm_->FetchPageRead(root_page_id);
//   guard = std::nullopt;  // release head
//
//   auto node = node_guard.As<BPlusTreePage>();
//   while (!node->IsLeafPage()) {
//     auto inner_node = node_guard.As<InternalPage>();
//     int next_id = inner_node->KeyIndex(key, comparator_);
//     page_id_t next_page_id = inner_node->ValueAt(next_id);
//     node_guard = bpm_->FetchPageRead(next_page_id);
//     node = node_guard.As<BPlusTreePage>();
//   }
//   return std::move(node_guard);
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(LeafPage *left, LeafPage *right, bool left_to_right) -> KeyType {
  size_t left_size = left->GetSize();
  if (left_to_right) {
    right->InsertAtFront(left->PairAt(left_size - 1));
    left->RemovePairAt(left_size - 1);
    return right->KeyAt(0);
  }
  left->InsertAtBack(right->PairAt(0));
  right->RemovePairAt(0);
  return right->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(InternalPage *left, InternalPage *right, bool left_to_right) -> KeyType {
  size_t left_size = left->GetSize();
  if (left_to_right) {
    right->InsertAtFront(left->PairAt(left_size - 1));
    left->RemovePairAt(left_size - 1);
    return right->KeyAt(0);
  }
  left->InsertAtBack(right->PairAt(0));
  right->RemovePairAt(0);
  return right->KeyAt(0);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
