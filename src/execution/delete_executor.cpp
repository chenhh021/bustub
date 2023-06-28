//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  txn_ = exec_ctx->GetTransaction();
}

void DeleteExecutor::Init() {
  child_executor_->Init();

  table_oid_t table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  auto table_name = table_info_->name_;

  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);

  int delete_num = 0;

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, child_rid);
    // for mvcc
    if (txn_ != nullptr) {
      txn_->LockTxn();
      TableWriteRecord record{plan_->TableOid(), child_rid, table_info_->table_.get()};
      record.wtype_ = WType::DELETE;
      txn_->AppendTableWriteRecord(record);
      txn_->UnlockTxn();
    }
    for (auto index : indexes) {
      auto key_schema = index->index_->GetKeySchema();
      auto attrs = index->index_->GetKeyAttrs();
      Tuple key = child_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs);
      index->index_->DeleteEntry(key, child_rid, nullptr);
    }
    ++delete_num;
  }

  executed_ = true;

  std::vector<Value> single_int_value{{TypeId::INTEGER, delete_num}};
  single_int_value.reserve(1);
  *tuple = Tuple{single_int_value, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
