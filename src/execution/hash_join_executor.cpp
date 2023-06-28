//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple right_tuple;
  RID right_rid;

  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto join_key = MakeJoinKey(plan_->RightJoinKeyExpressions(), right_child_->GetOutputSchema(), &right_tuple);
    if (right_tuples_.count(join_key) == 0) {
      right_tuples_[join_key] = std::vector<Tuple>();
    }
    right_tuples_[join_key].push_back(right_tuple);
  }

  left_status_ = left_child_->Next(&left_tuple_, &left_rid_);
  left_key_ = MakeJoinKey(plan_->LeftJoinKeyExpressions(), left_child_->GetOutputSchema(), &left_tuple_);
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_status_) {
    return false;
  }

  while (left_status_) {
    if (right_tuples_.count(left_key_) == 0) {
      bool out_flag = false;
      if (plan_->GetJoinType() == JoinType::LEFT) {
        CombineTuple(&left_tuple_, left_child_->GetOutputSchema(), nullptr, right_child_->GetOutputSchema(), tuple,
                     true);
        out_flag = true;
      }
      left_status_ = left_child_->Next(&left_tuple_, &left_rid_);
      left_key_ = MakeJoinKey(plan_->LeftJoinKeyExpressions(), left_child_->GetOutputSchema(), &left_tuple_);
      pkg_idx_ = 0;
      if (out_flag) {
        return true;
      }
    } else {
      auto pkg_size = right_tuples_[left_key_].size();
      if (pkg_idx_ < pkg_size) {
        CombineTuple(&left_tuple_, left_child_->GetOutputSchema(), &right_tuples_[left_key_][pkg_idx_++],
                     right_child_->GetOutputSchema(), tuple, false);
        return true;
      }

      left_status_ = left_child_->Next(&left_tuple_, &left_rid_);
      left_key_ = MakeJoinKey(plan_->LeftJoinKeyExpressions(), left_child_->GetOutputSchema(), &left_tuple_);
      pkg_idx_ = 0;
    }
  }
  return false;
}

}  // namespace bustub
