//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto aggregate_key = MakeAggregateKey(&tuple);
    auto aggregate_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    // handle empty table
    if (!executed_) {
      executed_ = true;
      auto initial_values = aht_.GenerateInitialAggregateValue();
      if (initial_values.aggregates_.size() != GetOutputSchema().GetColumnCount()) {
        return false;
      }
      *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
      return true;
    }
    return false;
  }

  if (!executed_) {
    executed_ = true;
  }

  std::vector<Value> values(aht_iterator_.Key().group_bys_);
  for (const auto &aggregate_value : aht_iterator_.Val().aggregates_) {
    values.push_back(aggregate_value);
  }
  *tuple = {values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
