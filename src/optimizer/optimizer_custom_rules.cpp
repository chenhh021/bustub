#include "execution/executors/filter_executor.h"
#include "execution/executors/values_executor.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeConstantFolding(p);
  p = OptimizeColumnPruning(p);
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizePredicatePushDown(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeIndexLookUp(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::OptimizeConstantFolding(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeConstantFolding(child));
  }
  AbstractPlanNodeRef optimized_plan = nullptr;

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(plan.get());
    Value const_val;
    if (IsExpressionConstant(nlj_plan->predicate_, const_val)) {
      // the predicate always passes
      if (const_val.GetAs<bool>()) {
        AbstractExpressionRef optimized_predicate =
            std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(CmpBool::CmpTrue));
        return std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(nlj_plan->OutputSchema()),
                                                        nlj_plan->GetLeftPlan(), nlj_plan->GetRightPlan(),
                                                        optimized_predicate, nlj_plan->GetJoinType());
      }

      // the predicate always fails
      // left join, replace right child by a dummy node
      if (nlj_plan->GetJoinType() == JoinType::LEFT) {
        auto right_plan = nlj_plan->GetRightPlan();
        std::vector<std::vector<AbstractExpressionRef>> empty_values;
        auto optimized_right_plan =
            std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(right_plan->OutputSchema()), empty_values);
        return std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(nlj_plan->OutputSchema()),
                                                        nlj_plan->GetLeftPlan(), optimized_right_plan,
                                                        nlj_plan->predicate_, nlj_plan->GetJoinType());
      }
      // inner join, return a dummy plan with same output schema
      if (nlj_plan->GetJoinType() == JoinType::INNER) {
        std::vector<std::vector<AbstractExpressionRef>> empty_values;
        return std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(plan->OutputSchema()), empty_values);
      }
      // other join type is not supported now
    }
  }

  if (plan->GetType() == PlanType::Filter) {
    Value const_val;
    const auto filter_plan = dynamic_cast<const FilterPlanNode *>(plan.get());
    if (IsExpressionConstant(filter_plan->predicate_, const_val)) {
      // predicate always passes
      if (const_val.GetAs<bool>()) {
        return plan->GetChildAt(0);
      }
      // predicate always fails
      std::vector<std::vector<AbstractExpressionRef>> empty_values;
      return std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(plan->OutputSchema()), empty_values);
    }
  }

  if (optimized_plan == nullptr) {
    optimized_plan = plan->CloneWithChildren(children);
  }
  return optimized_plan;
}

auto Optimizer::IsExpressionConstant(const AbstractExpressionRef &expression, Value &constant_val) -> bool {
  // the case expression is arithmetic expression
  auto arithmetic_expr = dynamic_cast<ArithmeticExpression *>(expression.get());
  if (arithmetic_expr != nullptr) {
    Value lhs;
    Value rhs;
    // left and right are all constant
    if (IsExpressionConstant(arithmetic_expr->GetChildAt(0), lhs) &&
        IsExpressionConstant(arithmetic_expr->GetChildAt(1), rhs)) {
      if (lhs.IsNull() || rhs.IsNull()) {
        constant_val = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      switch (arithmetic_expr->compute_type_) {
        case ArithmeticType::Plus:
          constant_val = ValueFactory::GetIntegerValue(lhs.GetAs<int32_t>() + rhs.GetAs<int32_t>());
          break;
        case ArithmeticType::Minus:
          constant_val = ValueFactory::GetIntegerValue(lhs.GetAs<int32_t>() - rhs.GetAs<int32_t>());
          break;
        default:
          UNREACHABLE("Unsupported arithmetic type.");
      }
      return true;
    }
    return false;
  }

  // the case expression is comparison expression
  auto comparison_expr = dynamic_cast<ComparisonExpression *>(expression.get());
  if (comparison_expr != nullptr) {
    Value lhs;
    Value rhs;
    // left and right are all constant
    if (IsExpressionConstant(comparison_expr->GetChildAt(0), lhs) &&
        IsExpressionConstant(comparison_expr->GetChildAt(1), rhs)) {
      CmpBool tmp_result;
      switch (comparison_expr->comp_type_) {
        case ComparisonType::Equal:
          tmp_result = lhs.CompareEquals(rhs);
          break;
        case ComparisonType::NotEqual:
          tmp_result = lhs.CompareNotEquals(rhs);
          break;
        case ComparisonType::LessThan:
          tmp_result = lhs.CompareLessThan(rhs);
          break;
        case ComparisonType::LessThanOrEqual:
          tmp_result = lhs.CompareLessThanEquals(rhs);
          break;
        case ComparisonType::GreaterThan:
          tmp_result = lhs.CompareGreaterThan(rhs);
          break;
        case ComparisonType::GreaterThanOrEqual:
          tmp_result = lhs.CompareGreaterThanEquals(rhs);
          break;
        default:
          BUSTUB_ASSERT(false, "Unsupported comparison type.");
      }
      constant_val = ValueFactory::GetBooleanValue(tmp_result);
      return true;
    }
    return false;
  }

  // the case expression is constant expression, return almost directly
  auto constant_expr = dynamic_cast<ConstantValueExpression *>(expression.get());
  if (constant_expr != nullptr) {
    constant_val = constant_expr->val_;
    return true;
  }

  // the case expression is logic expression
  auto logic_expr = dynamic_cast<LogicExpression *>(expression.get());
  if (logic_expr != nullptr) {
    Value lhs;
    Value rhs;

    auto get_bool_as_cmp_bool = [](const Value &val) -> CmpBool {
      if (val.IsNull()) {
        return CmpBool::CmpNull;
      }
      if (val.GetAs<bool>()) {
        return CmpBool::CmpTrue;
      }
      return CmpBool::CmpFalse;
    };

    auto is_l_const = IsExpressionConstant(logic_expr->GetChildAt(0), lhs);
    auto is_r_const = IsExpressionConstant(logic_expr->GetChildAt(1), rhs);

    auto l = get_bool_as_cmp_bool(lhs);
    auto r = get_bool_as_cmp_bool(rhs);
    switch (logic_expr->logic_type_) {
      case LogicType::And:
        // left and right are both constants
        if (is_l_const && is_r_const) {
          if (l == CmpBool::CmpFalse || r == CmpBool::CmpFalse) {
            constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpFalse);
            return true;
          }
          if (l == CmpBool::CmpTrue && r == CmpBool::CmpTrue) {
            constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpTrue);
            return true;
          }
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpNull);
          return true;
        }

        // one side is constant
        if (is_l_const && l == CmpBool::CmpFalse) {
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpFalse);
          return true;
        }

        if (is_r_const && r == CmpBool::CmpFalse) {
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpFalse);
          return true;
        }

        return false;
      case LogicType::Or:
        // when both sides are constants
        if (is_l_const && is_r_const) {
          if (l == CmpBool::CmpFalse && r == CmpBool::CmpFalse) {
            constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpFalse);
            return true;
          }
          if (l == CmpBool::CmpTrue || r == CmpBool::CmpTrue) {
            constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpTrue);
            return true;
          }
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpNull);
          return true;
        }

        // when only one side is constants
        if (is_l_const && l == CmpBool::CmpTrue) {
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpTrue);
          return true;
        }

        if (is_r_const && r == CmpBool::CmpTrue) {
          constant_val = ValueFactory::GetBooleanValue(CmpBool::CmpTrue);
          return true;
        }
        return false;
      default:
        UNREACHABLE("Unsupported logic type.");
    }
    return false;
  }

  return false;
};

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // note in this rule, optimization is from top to bottom
  std::vector<AbstractPlanNodeRef> children;
  if (plan->GetType() == PlanType::Projection) {
    auto proj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
    auto child_type = proj_plan->GetChildPlan()->GetType();
    // only handle the case that child changes column num
    if (child_type == PlanType::Aggregation || child_type == PlanType::Projection) {
      std::set<u_int32_t> idx_sets;
      RelatedChildColumnsExtract(proj_plan->GetExpressions(), idx_sets);

      // never prune group_bys
      if (child_type == PlanType::Aggregation) {
        auto child_plan = dynamic_cast<const AggregationPlanNode *>(proj_plan->GetChildPlan().get());
        assert(child_plan != nullptr);
        for (u_int32_t i = 0; i < child_plan->GetGroupBys().size(); ++i) {
          idx_sets.insert(i);
        }
      }

      std::vector<u_int32_t> pruned_child_idx;
      std::unordered_map<u_int32_t, u_int32_t> idx_map;
      u_int32_t mapped_idx = 0;
      for (auto idx : idx_sets) {
        pruned_child_idx.push_back(idx);
        idx_map[idx] = mapped_idx++;
      }

      if (child_type == PlanType::Aggregation) {
        ChildAggregateIndexSetCompact(proj_plan->GetChildPlan(), pruned_child_idx, idx_map);
      }

      // modify expression
      std::vector<AbstractExpressionRef> modified_expr;
      for (const auto &expr : proj_plan->GetExpressions()) {
        modified_expr.push_back(ModifyExpressions(expr, idx_map));
      }

      // optimize child
      auto optimized_child = OutputColumnPruning(proj_plan->GetChildPlan(), pruned_child_idx);
      optimized_child = OptimizeColumnPruning(optimized_child);

      return std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(proj_plan->OutputSchema()), modified_expr,
                                                  optimized_child);
    }
  }

  if (plan->GetType() == PlanType::Aggregation) {
    auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan.get());
    auto child_type = agg_plan->GetChildPlan()->GetType();
    // only handle the case that child changes column num
    if (child_type == PlanType::Aggregation || child_type == PlanType::Projection) {
      std::set<u_int32_t> idx_sets;
      RelatedChildColumnsExtract(agg_plan->GetGroupBys(), idx_sets);
      RelatedChildColumnsExtract(agg_plan->GetAggregates(), idx_sets);

      // never prune group_bys
      if (child_type == PlanType::Aggregation) {
        auto child_plan = dynamic_cast<const AggregationPlanNode *>(agg_plan->GetChildPlan().get());
        assert(child_plan != nullptr);
        for (u_int32_t i = 0; i < child_plan->GetGroupBys().size(); ++i) {
          idx_sets.insert(i);
        }
      }

      std::vector<u_int32_t> pruned_child_idx;
      std::unordered_map<u_int32_t, u_int32_t> idx_map;
      u_int32_t mapped_idx = 0;
      for (auto idx : idx_sets) {
        pruned_child_idx.push_back(idx);
        idx_map[idx] = mapped_idx++;
      }

      if (child_type == PlanType::Aggregation) {
        ChildAggregateIndexSetCompact(agg_plan->GetChildPlan(), pruned_child_idx, idx_map);
      }

      // modify expression
      std::vector<AbstractExpressionRef> modified_group_bys;
      for (const auto &group_by : agg_plan->GetGroupBys()) {
        modified_group_bys.push_back(ModifyExpressions(group_by, idx_map));
      }

      std::vector<AbstractExpressionRef> modified_aggregates;
      for (const auto &aggregate : agg_plan->GetAggregates()) {
        modified_aggregates.push_back(ModifyExpressions(aggregate, idx_map));
      }

      // optimize child
      auto optimized_child = OutputColumnPruning(agg_plan->GetChildPlan(), pruned_child_idx);
      optimized_child = OptimizeColumnPruning(optimized_child);

      return std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(agg_plan->OutputSchema()), optimized_child,
                                                   modified_group_bys, modified_aggregates,
                                                   agg_plan->GetAggregateTypes());
    }
  }

  // none of above case, children output need no prune
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }

  return plan->CloneWithChildren(children);
}

auto Optimizer::OutputColumnPruning(const AbstractPlanNodeRef &plan, const std::vector<uint32_t> &remains)
    -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    auto proj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());

    auto out_schema = std::make_shared<Schema>(Schema::CopySchema(&plan->OutputSchema(), remains));
    std::vector<AbstractExpressionRef> pruned_expr;
    pruned_expr.reserve(remains.size());
    for (auto idx : remains) {
      pruned_expr.push_back(proj_plan->GetExpressions()[idx]);
    }
    return std::make_shared<ProjectionPlanNode>(out_schema, pruned_expr, proj_plan->GetChildPlan());
  }

  if (plan->GetType() == PlanType::Aggregation) {
    auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan.get());

    auto out_schema = std::make_shared<Schema>(Schema::CopySchema(&plan->OutputSchema(), remains));

    auto group_by_size = agg_plan->GetGroupBys().size();
    std::vector<AbstractExpressionRef> pruned_aggregates;
    pruned_aggregates.reserve(remains.size() - group_by_size);
    for (auto idx : remains) {
      // the first group_by_size columns of aggregate results are group-by columns
      if (idx >= group_by_size) {
        pruned_aggregates.push_back(agg_plan->GetAggregates()[idx - group_by_size]);
      }
    }

    std::vector<AggregationType> pruned_aggregate_types;
    pruned_aggregate_types.reserve(remains.size() - group_by_size);
    for (auto idx : remains) {
      // the first group_by_size columns of aggregate results are group-by columns
      if (idx >= group_by_size) {
        pruned_aggregate_types.push_back(agg_plan->GetAggregateTypes()[idx - group_by_size]);
      }
    }
    return std::make_shared<AggregationPlanNode>(out_schema, agg_plan->GetChildPlan(), agg_plan->GetGroupBys(),
                                                 pruned_aggregates, pruned_aggregate_types);
  }

  return plan;
}

void Optimizer::RelatedChildColumnsExtract(const std::vector<AbstractExpressionRef> &expressions,
                                           std::set<u_int32_t> &idx_sets) {
  for (const auto &expr : expressions) {
    // order doesn't matter, so just level_traverse
    std::queue<AbstractExpressionRef> queue;
    queue.push(expr);
    while (!queue.empty()) {
      auto cur_expr = queue.front();
      queue.pop();
      // only cares about column value expression, and only column id matters now
      auto col_expr = dynamic_cast<ColumnValueExpression *>(cur_expr.get());
      if (col_expr != nullptr) {
        idx_sets.insert(col_expr->GetColIdx());
        continue;  // col_value_expr has no child
      }

      for (const auto &child : cur_expr->GetChildren()) {
        queue.push(child);
      }
    }
  }
}

auto Optimizer::ModifyExpressions(const AbstractExpressionRef &expr, std::unordered_map<u_int32_t, u_int32_t> map)
    -> AbstractExpressionRef {
  // recursively modify children
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.push_back(ModifyExpressions(child, map));
  }

  // only cares about column value expression, and only column id matters now
  auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
  // current expression is a column value expression
  if (col_expr != nullptr) {
    return std::make_shared<ColumnValueExpression>(col_expr->GetTupleIdx(), map[col_expr->GetColIdx()],
                                                   col_expr->GetReturnType());
  }
  // otherwise, return the expression with modified children
  return expr->CloneWithChildren(children);
}

void Optimizer::ChildAggregateIndexSetCompact(const AbstractPlanNodeRef &plan, std::vector<u_int32_t> &idx_set,
                                              std::unordered_map<u_int32_t, u_int32_t> &idx_map) {
  auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan.get());
  auto group_by_size = agg_plan->GetGroupBys().size();
  std::set<u_int32_t> agg_idx_set;
  std::set<u_int32_t> compact_result_set;
  std::unordered_map<u_int32_t, u_int32_t> repeat_idx_map;
  // n^2 algorithm, but executed once and the scope is limited, so it's okay
  for (u_int32_t idx : idx_set) {
    if (idx < group_by_size) {
      compact_result_set.insert(idx);
      continue;
    }

    auto agg_idx = idx - group_by_size;
    bool repeat_flag = false;
    for (u_int32_t exist_idx : agg_idx_set) {
      if (agg_plan->GetAggregateTypes()[exist_idx] == agg_plan->GetAggregateTypes()[agg_idx] &&
          ColumnEqual(agg_plan->GetAggregates()[exist_idx], agg_plan->GetAggregates()[agg_idx])) {
        repeat_idx_map[agg_idx + group_by_size] = exist_idx + group_by_size;
        repeat_flag = true;
        break;
      }
    }

    if (!repeat_flag) {
      agg_idx_set.insert(agg_idx);
    }
  }

  for (u_int32_t idx : agg_idx_set) {
    compact_result_set.insert(idx + group_by_size);
  }
  idx_set = std::vector<u_int32_t>(compact_result_set.begin(), compact_result_set.end());

  std::set<u_int32_t> ori_key_set;
  for (auto [idx, _] : idx_map) {
    ori_key_set.insert(idx);
  }

  size_t mapped_idx = 0;
  for (auto idx : ori_key_set) {
    if (repeat_idx_map.count(idx) == 0) {
      idx_map[idx] = mapped_idx++;
    } else {
      idx_map[idx] = idx_map[repeat_idx_map[idx]];
    }
  }
}

// judge whether the two expressions are equal when both of them are column value expression
inline auto Optimizer::ColumnEqual(const AbstractExpressionRef &a, const AbstractExpressionRef &b) -> bool {
  auto col_expr_a = dynamic_cast<ColumnValueExpression *>(a.get());
  auto col_expr_b = dynamic_cast<ColumnValueExpression *>(b.get());
  if (col_expr_a == nullptr || col_expr_b == nullptr) {
    return false;
  }

  return col_expr_a->GetTupleIdx() == col_expr_b->GetTupleIdx() && col_expr_a->GetColIdx() == col_expr_b->GetColIdx();
}

auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // when predicate is pushed to the current node, an additional filter node is added, merge it if the following node is
  // a NestedLoopJoin node
  auto merged_plan = OptimizeMergeFilterNLJ(plan);

  if (merged_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(merged_plan.get());
    AbstractExpressionRef left_predicate;
    AbstractExpressionRef right_predicate;
    AbstractExpressionRef rem_predicate;
    ClassifyPredicate(nlj_plan->Predicate(), left_predicate, right_predicate, rem_predicate);
    if (rem_predicate == nullptr) {
      rem_predicate = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(CmpBool::CmpTrue));
    }
    AbstractPlanNodeRef left_child = nullptr;
    AbstractPlanNodeRef right_child = nullptr;

    if (left_predicate != nullptr) {
      left_child = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(nlj_plan->GetLeftPlan()->OutputSchema()),
                                                    left_predicate, nlj_plan->GetLeftPlan());
    } else {
      left_child = nlj_plan->GetLeftPlan();
    }

    if (right_predicate != nullptr) {
      right_child = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(nlj_plan->GetRightPlan()->OutputSchema()),
                                                     right_predicate, nlj_plan->GetRightPlan());
    } else {
      right_child = nlj_plan->GetRightPlan();
    }
    // remember optimize children
    return std::make_shared<NestedLoopJoinPlanNode>(
        std::make_shared<Schema>(nlj_plan->OutputSchema()), OptimizePredicatePushDown(left_child),
        OptimizePredicatePushDown(right_child), rem_predicate, nlj_plan->GetJoinType());
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizePredicatePushDown(child));
  }

  return plan->CloneWithChildren(children);
}

void Optimizer::DecomposePredicateByLogicAnd(const AbstractExpressionRef &expr,
                                             std::vector<std::vector<AbstractExpressionRef>> &decomposed_lists) {
  // decomposed_lists = {left_child_list, right_child_list, remaining_list}
  assert(decomposed_lists.size() == 3);
  // all expression except and-logic expression is treated as leaf; use level traverse to decompose
  std::queue<AbstractExpressionRef> que;
  que.push(expr);
  while (!que.empty()) {
    auto cur = que.front();
    que.pop();

    auto logic_node = dynamic_cast<LogicExpression *>(cur.get());
    if (logic_node != nullptr && logic_node->logic_type_ == LogicType::And) {
      que.push(logic_node->GetChildAt(0));
      que.push(logic_node->GetChildAt(1));
    } else {
      int type = PredicateRelation(cur);
      type = type == -1 ? 2 : type;  // keep the constant predicate to current node if exists
      decomposed_lists[type].push_back(cur);
    }
  }
}

// 0: related only to the left children; 1: related only to the right child; 2: related to both children; -1: constant
auto Optimizer::PredicateRelation(const AbstractExpressionRef &expr) -> int {
  auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
  if (col_expr != nullptr) {
    if (col_expr->GetTupleIdx() > 1) {
      // should not happen
      return -1;
    }

    return col_expr->GetTupleIdx();
  }

  bool left = false;
  bool right = false;

  for (const auto &child : expr->GetChildren()) {
    int child_type = PredicateRelation(child);
    switch (child_type) {
      case -1:
        break;
      case 0:
        left = true;
        break;
      case 1:
        right = true;
        break;
      default:
        return 2;
    }
  }

  if (!left && !right) {
    return -1;
  }

  if (left && !right) {
    return 0;
  }

  if (!left && right) {
    return 1;
  }

  return 2;
}

void Optimizer::ClassifyPredicate(const AbstractExpressionRef &all, AbstractExpressionRef &left,
                                  AbstractExpressionRef &right, AbstractExpressionRef &self) {
  std::vector<std::vector<AbstractExpressionRef>> decomposed_lists(3);
  decomposed_lists.reserve(3);

  DecomposePredicateByLogicAnd(all, decomposed_lists);
  left = ComposePredicate(decomposed_lists[0], false);
  right = ComposePredicate(decomposed_lists[1], true);  // change 1.x to 0.x in predicate
  self = ComposePredicate(decomposed_lists[2], false);
}

auto Optimizer::ComposePredicate(const std::vector<AbstractExpressionRef> &list, bool unify_column)
    -> AbstractExpressionRef {
  if (list.empty()) {
    return nullptr;
  }

  auto base_expr = list[0];
  if (unify_column) {
    base_expr = UnifyColumn(base_expr);
  }

  for (size_t i = 1; i < list.size(); ++i) {
    auto extend_expr = list[i];
    if (unify_column) {
      extend_expr = UnifyColumn(extend_expr);
    }
    base_expr = std::make_shared<LogicExpression>(base_expr, extend_expr, LogicType::And);
  }

  return base_expr;
}

// change 1.x to 0.x in predicate
auto Optimizer::UnifyColumn(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  auto col_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
  if (col_expr != nullptr) {
    return std::make_shared<ColumnValueExpression>(0, col_expr->GetColIdx(), col_expr->GetReturnType());
  }

  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.push_back(UnifyColumn(child));
  }

  return expr->CloneWithChildren(children);
}

auto Optimizer::OptimizeIndexLookUp(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode *>(plan.get());
  // only optimize SeqScanPlanNode with filter predicate
  if (seq_scan_plan == nullptr || seq_scan_plan->filter_predicate_ == nullptr) {
    return plan;
  }

  // only supple integer index now
  std::vector<std::pair<int32_t, int32_t>> range;
  std::unordered_map<u_int32_t, u_int32_t> col_position;
  bool is_index_lookup_suitable = PredicateProcessForIndexLookUp(seq_scan_plan->filter_predicate_, range, col_position);

  if (!is_index_lookup_suitable) {
    return plan;
  }

  const auto *table_info = catalog_.GetTable(seq_scan_plan->GetTableOid());
  const auto indices = catalog_.GetTableIndexes(table_info->name_);
  // find first match index, not look for best match at present
  for (const auto *index : indices) {
    const auto &key_attrs = index->index_->GetKeyAttrs();
    // check index key schema matches columns having range predicates
    bool valid = true;
    if (key_attrs.size() <= range.size()) {
      for (auto key_attr : key_attrs) {
        if (col_position.count(key_attr) == 0) {
          valid = false;
          break;
        }
      }
      if (valid) {
        std::vector<std::pair<int32_t, int32_t>> reordered_range;
        reordered_range.reserve(key_attrs.size());
        for (auto key_attr : key_attrs) {
          reordered_range.emplace_back(range[col_position[key_attr]]);
        }
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan->output_schema_, index->index_oid_,
                                                   seq_scan_plan->filter_predicate_, reordered_range);
      }
    }
  }

  return plan;
}

auto Optimizer::PredicateProcessForIndexLookUp(const AbstractExpressionRef &predicate,
                                               std::vector<std::pair<int32_t, int32_t>> &range,
                                               std::unordered_map<u_int32_t, u_int32_t> &col_position) -> bool {
  // order doesn't matter, so just level_traverse
  std::queue<AbstractExpressionRef> que;
  que.push(predicate);
  u_int32_t idx = 0;
  bool flag = true;
  // all expression except and-logic expression is treated as leaf; use level traverse to decompose
  while (!que.empty()) {
    auto cur = que.front();
    que.pop();

    auto logic_node = dynamic_cast<LogicExpression *>(cur.get());
    if (logic_node != nullptr && logic_node->logic_type_ == LogicType::And) {
      que.push(logic_node->GetChildAt(0));
      que.push(logic_node->GetChildAt(1));
    } else {
      // evaluate whether the expression is x.x >=< const
      auto cmp_expr = dynamic_cast<ComparisonExpression *>(cur.get());
      if (cmp_expr == nullptr) {
        continue;
      }

      auto left_node = cmp_expr->GetChildAt(0);
      auto right_node = cmp_expr->GetChildAt(1);
      auto left_col = dynamic_cast<ColumnValueExpression *>(left_node.get());
      auto left_const = dynamic_cast<ConstantValueExpression *>(left_node.get());
      auto right_col = dynamic_cast<ColumnValueExpression *>(right_node.get());
      auto right_const = dynamic_cast<ConstantValueExpression *>(right_node.get());

      auto cmp_type = cmp_expr->comp_type_;
      // left child is col value and right child is constant
      if (left_col != nullptr && right_const != nullptr) {
        // only NotEqual has no contribute to range search
        if (cmp_type == ComparisonType::NotEqual) {
          continue;
        }

        // tuple id should not matter, as here the columns come from the same table when look up indexes
        auto col_attr = left_col->GetColIdx();
        auto value = right_const->val_.GetAs<int32_t>();  // only support int32 index now
        if (col_position.count(col_attr) == 0) {
          col_position[col_attr] = idx++;
          range.emplace_back(BUSTUB_INT32_MIN, BUSTUB_INT32_MAX);
        }
        size_t col_idx = col_position[col_attr];

        switch (cmp_type) {
          case ComparisonType::Equal:
            range[col_idx].first = value;
            range[col_idx].second = value;
            flag = true;
            break;
          case ComparisonType::NotEqual:
            // do nothing
            break;
          case ComparisonType::LessThan:
            range[col_idx].second = value - 1;
            break;
          case ComparisonType::LessThanOrEqual:
            range[col_idx].second = value;
            break;
          case ComparisonType::GreaterThan:
            range[col_idx].first = value + 1;
            break;
          case ComparisonType::GreaterThanOrEqual:
            range[col_idx].first = value;
            break;
        }
      }

      // left child is constant and right child is column value
      if (left_const != nullptr && right_col != nullptr) {
        // only NotEqual has no contribute to range search
        if (cmp_type == ComparisonType::NotEqual) {
          continue;
        }

        // tuple id should not matter, as here the columns come from the same table when look up indexes
        auto col_attr = right_col->GetColIdx();
        auto value = left_const->val_.GetAs<int32_t>();  // only support int32 index now
        if (col_position.count(col_attr) == 0) {
          col_position[col_attr] = idx++;
          range.emplace_back(BUSTUB_INT32_MIN, BUSTUB_INT32_MAX);
        }
        size_t col_idx = col_position[col_attr];

        switch (cmp_type) {
          case ComparisonType::Equal:
            range[col_idx].first = value;
            range[col_idx].second = value;
            flag = true;
            break;
          case ComparisonType::NotEqual:
            // do nothing
            break;
          case ComparisonType::LessThan:
            range[col_idx].first = value + 1;
            break;
          case ComparisonType::LessThanOrEqual:
            range[col_idx].first = value;
            break;
          case ComparisonType::GreaterThan:
            range[col_idx].second = value - 1;
            break;
          case ComparisonType::GreaterThanOrEqual:
            range[col_idx].second = value;
            break;
        }
      }
    }
  }

  if (!flag) {
    bool left_flag = true;
    bool right_flag = true;
    for (auto [l, r] : range) {
      if (l == BUSTUB_INT32_MIN) {
        left_flag = false;
      }

      if (r == BUSTUB_INT32_MAX) {
        right_flag = false;
      }
    }

    flag = left_flag || right_flag;
  }

  return flag;
}

}  // namespace bustub
