//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  int modified_num = 0;

  Tuple child_tuple;
  RID child_rid;

  auto table_name = table_info_->name_;
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> values{};
    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = {values, &child_executor_->GetOutputSchema()};

    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, child_rid);
    auto new_rid = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple);
    BUSTUB_ASSERT(new_rid.has_value(), "insert tuple fails when update");
    for (auto index : indexes) {
      auto key_schema = index->index_->GetKeySchema();
      auto attrs = index->index_->GetKeyAttrs();
      Tuple old_key = child_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs);
      Tuple new_key = new_tuple.KeyFromTuple(table_info_->schema_, *key_schema, attrs);
      index->index_->DeleteEntry(old_key, child_rid, nullptr);
      index->index_->InsertEntry(new_key, *new_rid, nullptr);
    }
    ++modified_num;
  }

  executed_ = true;
  std::vector<Value> single_int_value{{TypeId::INTEGER, modified_num}};
  single_int_value.reserve(1);
  *tuple = Tuple{single_int_value, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
