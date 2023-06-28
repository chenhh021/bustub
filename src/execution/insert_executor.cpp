//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  txn_ = exec_ctx->GetTransaction();
}

void InsertExecutor::Init() {
  child_executor_->Init();

  table_oid_t table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  txn_ = exec_ctx_->GetTransaction();
  // for mvcc
  if (txn_ != nullptr) {
    bool lock_status = false;
    try {
      lock_status = exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Grant IX table lock fails");
    }

    if (!lock_status) {
      throw ExecutionException("Grant IX table lock fails");
    }
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  int insert_num = 0;

  auto table_name = table_info_->name_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // insert tuple to table_heap
    auto insert_rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple,
                                                       exec_ctx_->GetLockManager(), txn_, plan_->TableOid());
    if (insert_rid == std::nullopt) {
      continue;
    }
    // for mvcc
    if (txn_ != nullptr) {
      txn_->LockTxn();
      TableWriteRecord record{plan_->TableOid(), insert_rid.value(), table_info_->table_.get()};
      record.wtype_ = WType::INSERT;
      txn_->AppendTableWriteRecord(record);
      txn_->UnlockTxn();
    }
    ++insert_num;
    // update indexes
    for (auto index : indexes) {
      auto key_schema = index->index_->GetKeySchema();
      auto attrs = index->index_->GetKeyAttrs();
      Tuple key = child_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs);
      index->index_->InsertEntry(key, *insert_rid, nullptr);
    }
  }

  executed_ = true;

  std::vector<Value> single_int_value{{TypeId::INTEGER, insert_num}};
  single_int_value.reserve(1);
  *tuple = Tuple{single_int_value, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
