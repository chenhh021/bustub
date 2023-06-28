//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_oid_t index_oid = plan_->GetIndexOid();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);

  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == std::nullopt) {
    auto it = tree_->GetBeginIterator();
    iter_.emplace(std::move(it));
  }

  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }

    *rid = (**iter_).second;
    ++(*iter_);
    auto tuple_pair = table_info_->table_->GetTuple(*rid);

    if (!tuple_pair.first.is_deleted_) {
      *tuple = std::move(tuple_pair.second);
      return true;
    }
  }
}

}  // namespace bustub
