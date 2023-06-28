//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.push_back(right_tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_status_) {
    return false;
  }

  while (true) {
    if (!left_status_) {
      return false;
    }

    bool right_status = right_idx_ != right_tuples_.size();

    if (right_status) {
      auto match = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                   &right_tuples_[right_idx_], right_executor_->GetOutputSchema());

      if (!match.IsNull() && match.GetAs<bool>()) {
        CombineTuple(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuples_[right_idx_],
                     right_executor_->GetOutputSchema(), tuple, false);
        padding_label_ = false;
        ++right_idx_;
        return true;
      }

      ++right_idx_;
    } else {
      right_idx_ = 0;
      if (plan_->GetJoinType() == JoinType::LEFT && padding_label_) {
        CombineTuple(&left_tuple_, left_executor_->GetOutputSchema(), nullptr, right_executor_->GetOutputSchema(),
                     tuple, true);
        left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
        right_executor_->Init();
        padding_label_ = true;
        return true;
      }
      // inner join or left join need no paddling out
      left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
      right_executor_->Init();
      padding_label_ = true;
    }
  }
}
void NestedLoopJoinExecutor::CombineTuple(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                                          const Schema &right_schema, Tuple *out_tuple, bool null_padding) {
  int left_size = left_schema.GetColumnCount();
  int right_size = right_schema.GetColumnCount();
  int out_size = GetOutputSchema().GetColumnCount();

  std::vector<Value> values;
  values.reserve(out_size);
  for (int i = 0; i < left_size; ++i) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }

  if (null_padding) {
    for (int i = 0; i < right_size; ++i) {
      values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
    }
  } else {
    for (int i = 0; i < right_size; ++i) {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    }
  }
  *out_tuple = {values, &GetOutputSchema()};
}

}  // namespace bustub
