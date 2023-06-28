#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    const auto predicate = nlj_plan.Predicate();
    const auto logic_expr = dynamic_cast<const LogicExpression *>(predicate.get());
    // potential case 2
    if (logic_expr != nullptr) {
      auto left_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(0).get());
      auto right_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(1).get());

      if (left_expr != nullptr && right_expr != nullptr) {
        if (left_expr->comp_type_ == ComparisonType::Equal && right_expr->comp_type_ == ComparisonType::Equal) {
          auto left_expr_0 = dynamic_cast<ColumnValueExpression *>(left_expr->GetChildAt(0).get());
          auto right_expr_0 = dynamic_cast<ColumnValueExpression *>(left_expr->GetChildAt(1).get());
          auto left_expr_1 = dynamic_cast<ColumnValueExpression *>(right_expr->GetChildAt(0).get());
          auto right_expr_1 = dynamic_cast<ColumnValueExpression *>(right_expr->GetChildAt(1).get());
          if (left_expr_0 != nullptr && right_expr_0 != nullptr && left_expr_1 != nullptr && right_expr_1 != nullptr) {
            std::vector<AbstractExpressionRef> left_keys;
            std::vector<AbstractExpressionRef> right_keys;

            auto left_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, left_expr_0->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, right_expr_0->GetColIdx(), right_expr->GetReturnType());
            auto left_expr_tuple_1 =
                std::make_shared<ColumnValueExpression>(0, left_expr_1->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple_1 =
                std::make_shared<ColumnValueExpression>(0, right_expr_1->GetColIdx(), right_expr->GetReturnType());

            if (left_expr_0->GetTupleIdx() == 0 && right_expr_0->GetTupleIdx() == 1) {
              left_keys.emplace_back(left_expr_tuple_0);
              right_keys.emplace_back(right_expr_tuple_0);
            } else {
              left_keys.emplace_back(right_expr_tuple_0);
              right_keys.emplace_back(left_expr_tuple_0);
            }

            if (left_expr_1->GetTupleIdx() == 0 && right_expr_1->GetTupleIdx() == 1) {
              left_keys.emplace_back(left_expr_tuple_1);
              right_keys.emplace_back(right_expr_tuple_1);
            } else {
              left_keys.emplace_back(right_expr_tuple_1);
              right_keys.emplace_back(left_expr_tuple_1);
            }
            return std::make_shared<HashJoinPlanNode>(std::make_shared<Schema>(nlj_plan.OutputSchema()),
                                                      nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), left_keys,
                                                      right_keys, nlj_plan.GetJoinType());
          }
        }
      }
    }

    auto cmp_expr = dynamic_cast<ComparisonExpression *>(predicate.get());
    // case 1
    if (cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
      auto left_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
      auto right_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
      if (left_expr != nullptr && right_expr != nullptr) {
        std::vector<AbstractExpressionRef> left_keys;
        std::vector<AbstractExpressionRef> right_keys;
        auto left_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
        auto right_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          left_keys.push_back(left_expr_tuple_0);
          right_keys.push_back(right_expr_tuple_0);
        } else {
          left_keys.push_back(right_expr_tuple_0);
          right_keys.push_back(left_expr_tuple_0);
        }
        return std::make_shared<HashJoinPlanNode>(std::make_shared<Schema>(nlj_plan.OutputSchema()),
                                                  nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), left_keys,
                                                  right_keys, nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
