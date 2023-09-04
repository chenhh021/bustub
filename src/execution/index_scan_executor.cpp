//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_oid_t index_oid = plan_->GetIndexOid();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);

  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());

  // handle range index scan
  if (!plan_->range_.empty()) {
    Schema *key_schema = tree_->GetKeySchema();
    std::vector<Value> values_begin;
    std::vector<Value> values_end;
    values_begin.reserve(plan_->range_.size());
    values_end.reserve(plan_->range_.size());
    for (auto &[begin, end] : plan_->range_) {
      values_begin.emplace_back(ValueFactory::GetIntegerValue(begin));
      values_end.emplace_back(ValueFactory::GetIntegerValue(end));
    }
    values_end.pop_back();
    // set end value the one exactly larger than the last valid value
    values_end.emplace_back(ValueFactory::GetIntegerValue(plan_->range_.back().second + 1));

    Tuple begin_key_tuple{values_begin, key_schema};
    IntegerKeyType key_begin;
    key_begin.SetFromKey(begin_key_tuple);
    key_begin_ = key_begin;

    Tuple end_key_tuple{values_end, key_schema};
    IntegerKeyType key_end;
    key_begin.SetFromKey(end_key_tuple);
    key_end_ = key_end;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == std::nullopt) {
    // index scan with range
    if (!plan_->range_.empty()) {
      assert(plan_->range_.size() <= 2);  // support at max index of 2 columns
      auto it = tree_->GetBeginIterator(key_begin_);
      iter_.emplace(std::move(it));
      auto it_end = tree_->GetBeginIterator(key_end_);
      end_iter_.emplace(std::move(it_end));
    } else {
      auto it = tree_->GetBeginIterator();
      iter_.emplace(std::move(it));
    }
  }

  while (true) {
    if (!plan_->range_.empty()) {
      if (iter_->IsEnd() || iter_ == end_iter_) {
        return false;
      }
    } else {
      if (iter_->IsEnd()) {
        return false;
      }
    }

    *rid = (**iter_).second;
    auto tuple_pair = table_info_->table_->GetTuple(*rid);
    // if index scan with range
    if (!tuple_pair.first.is_deleted_ && !plan_->range_.empty()) {
      auto key_attrs = index_info_->index_->GetKeyAttrs();
      std::vector<int32_t> raw_key_values;
      raw_key_values.reserve(key_attrs.size());
      for (auto key_attr : key_attrs) {
        auto raw_value = tuple_pair.second.GetValue(&plan_->OutputSchema(), key_attr);
        raw_key_values.emplace_back(raw_value.GetAs<int32_t>());
      }

      int up = 1;
      for (int i = raw_key_values.size() - 1; i >= 0; --i) {
        auto key_value = raw_key_values[i];
        key_value += up;
        if (key_value > plan_->range_[i].second) {
          key_value = plan_->range_[i].first;
          up = 1;
        }
        raw_key_values[i] = key_value;
      }

      std::vector<Value> key_values;
      key_values.reserve(raw_key_values.size());
      for (auto raw : raw_key_values) {
        key_values.emplace_back(ValueFactory::GetIntegerValue(raw));
      }

      Tuple next_key_tuple{key_values, &index_info_->key_schema_};
      IntegerKeyType next_valid_key;
      next_valid_key.SetFromKey(next_key_tuple);
      iter_.emplace(tree_->GetBeginIterator(next_valid_key));
    } else {
      ++(*iter_);
    }

    if (!tuple_pair.first.is_deleted_) {
      // handle predicate, in case that merge filter with seq scan
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&tuple_pair.second, plan_->OutputSchema());
        if (!value.IsNull() && !value.GetAs<bool>()) {
          continue;
        }
      }
      *tuple = std::move(tuple_pair.second);
      ++count_;
      return true;
    }
  }
}

}  // namespace bustub
