//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** JoinKey represents a key in an aggregation operation */
struct JoinKey {
  /** The key values */
  std::vector<Value> join_keys_;

  /**
   * Compares two join keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_keys_.size(); i++) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.join_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeJoinKey(const std::vector<AbstractExpressionRef> &key_expressions, const Schema &output_schema,
                   const Tuple *tuple) -> JoinKey {
    std::vector<Value> keys;
    keys.reserve(key_expressions.size());
    for (const auto &expr : key_expressions) {
      keys.emplace_back(expr->Evaluate(tuple, output_schema));
    }
    return {keys};
  }

  void CombineTuple(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
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

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  /** whether left executor has next tuple*/
  bool left_status_;
  Tuple left_tuple_;
  RID left_rid_;
  JoinKey left_key_;

  std::unordered_map<JoinKey, std::vector<Tuple>> right_tuples_;

  size_t pkg_idx_ = 0;
};

}  // namespace bustub
