#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple a, const Tuple b) -> bool {
    for (auto [type, expr] : plan_->GetOrderBy()) {
      if (type == OrderByType::DESC) {
        if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }

        if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }

        if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;
  });

  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *iter_++;
  return true;
}

}  // namespace bustub
