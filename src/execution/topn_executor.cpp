#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  std::vector<Tuple> tuples;

  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(tuple);
  }

  int heap_num = std::min(plan_->GetN(), tuples.size());
  heap_.reserve(heap_num);
  heap_.resize(heap_num);

  std::partial_sort_copy(
      tuples.begin(), tuples.end(), heap_.begin(), heap_.end(), [&](const Tuple a, const Tuple b) -> bool {
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
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ >= GetNumInHeap()) {
    return false;
  }

  *tuple = heap_[idx_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
