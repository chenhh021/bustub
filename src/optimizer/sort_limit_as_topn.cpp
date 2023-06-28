#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    if (optimized_plan->GetChildren().size() == 1) {
      auto child_plan = optimized_plan->GetChildren().begin()->get();
      if (child_plan->GetType() == PlanType::Sort) {
        auto limit_plan = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
        auto sort_plan = dynamic_cast<const SortPlanNode *>(child_plan);
        return std::make_shared<TopNPlanNode>(std::make_shared<Schema>(optimized_plan->OutputSchema()),
                                              child_plan->GetChildren()[0], sort_plan->GetOrderBy(),
                                              limit_plan->GetLimit());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
