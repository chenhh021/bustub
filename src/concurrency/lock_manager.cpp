//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <stack>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // log output
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                        ":attempt lock table " + std::to_string(oid) + ";lock mode:" + LockModeToString(lock_mode);
  LOG_DEBUG("%s", loginfo.c_str());
  // check whether compatible with isolation level
  if (!CanTxnTakeLock(txn, lock_mode)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    if (txn->GetState() == TransactionState::SHRINKING) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  // check whether already hold the same lock
  LockMode curr_lock_mode;

  bool lock_held = TableIsAlreadyLocked(txn, curr_lock_mode, oid);
  if (lock_held) {
    if (curr_lock_mode == lock_mode) {
      return true;
    }

    UpgradeLockTable(txn, lock_mode, oid);
  }

  std::unique_lock map_lock(table_lock_map_latch_);
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto request_queue = table_lock_map_[oid];
  map_lock.unlock();

  std::unique_lock queue_lock(request_queue->latch_);
  // either returned ture, or add request in the upgrade function
  if (!lock_held) {
    request_queue->request_queue_.emplace_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
  }
  //  queue_lock.unlock();

  LockRequestRef cur_request = nullptr;

  while (txn->GetState() != TransactionState::ABORTED &&
         !GrantNewLocksIsPossible(request_queue.get(), txn, cur_request)) {
    request_queue->cv_.wait(queue_lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
              ":txn aborted. clean lock table" + std::to_string(oid);
    LOG_DEBUG("%s", loginfo.c_str());

    auto txn_equal = [&](const LockRequestRef &request) -> bool {
      return request->txn_id_ == txn->GetTransactionId() && !request->granted_;
    };
    request_queue->request_queue_.remove_if(txn_equal);
    queue_lock.unlock();
    request_queue->cv_.notify_all();

    return false;
  }

  cur_request->granted_ = true;

  txn->LockTxn();
  // book keeping
  AddTxnTableLockLabel(txn, lock_mode, oid);
  txn->UnlockTxn();

  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":granted lock table " + std::to_string(oid) + ";lock mode:" + LockModeToString(lock_mode);
  LOG_DEBUG("%s", loginfo.c_str());

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  const int txn_id = txn->GetTransactionId();

  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn_id) +
                        ":attempt unlock table " + std::to_string(oid);
  LOG_DEBUG("%s", loginfo.c_str());

  std::unique_lock map_lock(table_lock_map_latch_);
  if (table_lock_map_.count(oid) == 0) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request_queue = table_lock_map_[oid];
  std::unique_lock queue_lock(request_queue->latch_);
  map_lock.unlock();
  LockRequestRef granted_request = nullptr;
  for (auto &request : request_queue->request_queue_) {
    if (request->granted_ && request->txn_id_ == txn_id) {
      granted_request = request;
      break;
    }
  }
  if (granted_request == nullptr) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  if ((txn->GetExclusiveRowLockSet()->count(oid) != 0 && !(*txn->GetExclusiveRowLockSet())[oid].empty()) ||
      (txn->GetSharedRowLockSet()->count(oid) != 0 && !(*txn->GetSharedRowLockSet())[oid].empty())) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  request_queue->request_queue_.remove(granted_request);
  request_queue->cv_.notify_all();

  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn_id) + ":released lock table " +
            std::to_string(oid);
  LOG_DEBUG("%s", loginfo.c_str());

  txn->LockTxn();
  queue_lock.unlock();
  LockMode cur_lock_mode = granted_request->lock_mode_;
  RemoveTxnTableLockLabel(txn, cur_lock_mode, oid);

  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        if (cur_lock_mode == LockMode::EXCLUSIVE || cur_lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
        if (cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }

  txn->UnlockTxn();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                        ":attempt lock row in table" + std::to_string(oid) + "." + rid.ToString() +
                        ";lock mode:" + LockModeToString(lock_mode);
  LOG_DEBUG("%s", loginfo.c_str());

  // check intention lock attempt
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // check whether compatible with isolation level
  if (!CanTxnTakeLock(txn, lock_mode)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    if (txn->GetState() == TransactionState::SHRINKING) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  // check weather hold required table lock
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // check whether already hold the same lock
  LockMode curr_lock_mode;
  bool lock_held = RowIsAlreadyLocked(txn, curr_lock_mode, oid, rid);

  if (lock_held) {
    if (curr_lock_mode == lock_mode) {
      return true;
    }
    UpgradeLockRow(txn, lock_mode, oid, rid);
  }

  std::unique_lock map_lock(row_lock_map_latch_);
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto request_queue = row_lock_map_[rid];
  map_lock.unlock();

  std::unique_lock queue_lock(request_queue->latch_);
  // either returned ture, or add request in the upgrade function
  if (!lock_held) {
    request_queue->request_queue_.emplace_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  }

  LockRequestRef cur_request = nullptr;
  while (txn->GetState() != TransactionState::ABORTED &&
         !GrantNewLocksIsPossible(request_queue.get(), txn, cur_request)) {
    loginfo = "Thread " + std::to_string(pthread_self()) + ":new lock waiting.";
    LOG_DEBUG("%s", loginfo.c_str());
    request_queue->cv_.wait(queue_lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
              ":txn aborted. clean lock table" + std::to_string(oid) + "." + rid.ToString();
    LOG_DEBUG("%s", loginfo.c_str());
    auto txn_equal = [&](const LockRequestRef &request) -> bool {
      return request->txn_id_ == txn->GetTransactionId() && !request->granted_;
    };
    request_queue->request_queue_.remove_if(txn_equal);
    queue_lock.unlock();
    request_queue->cv_.notify_all();
    return false;
  }

  cur_request->granted_ = true;

  txn->LockTxn();
  // book keeping
  AddTxnRowLockLabel(txn, lock_mode, oid, rid);
  txn->UnlockTxn();

  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":granted lock row in table" + std::to_string(oid) + "." + rid.ToString() +
            ";lock mode:" + LockModeToString(lock_mode);
  LOG_DEBUG("%s", loginfo.c_str());

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  const int txn_id = txn->GetTransactionId();
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn_id) +
                        ":attempt unlock row in table" + std::to_string(oid) + "." + rid.ToString();
  LOG_DEBUG("%s", loginfo.c_str());

  std::unique_lock map_lock(row_lock_map_latch_);
  if (row_lock_map_.count(rid) == 0) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request_queue = row_lock_map_[rid];

  std::unique_lock queue_lock(request_queue->latch_);
  map_lock.unlock();
  LockRequestRef granted_request = nullptr;
  for (auto &request : request_queue->request_queue_) {
    if (request->granted_ && request->txn_id_ == txn_id) {
      granted_request = request;
      break;
    }
  }
  if (granted_request == nullptr) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  request_queue->request_queue_.remove(granted_request);
  request_queue->cv_.notify_all();

  txn->LockTxn();
  queue_lock.unlock();
  RemoveTxnRowLockLabel(txn, granted_request->lock_mode_, oid, rid);

  if (!force && txn->GetState() == TransactionState::GROWING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (granted_request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }

  txn->UnlockTxn();

  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":released lock row in table" + std::to_string(oid) + "." + rid.ToString();
  LOG_DEBUG("%s", loginfo.c_str());

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::string loginfo =
      "Thread " + std::to_string(pthread_self()) + ":attempt add edge" + std::to_string(t1) + "->" + std::to_string(t2);
  LOG_DEBUG("%s", loginfo.c_str());
  if (waits_for_.count(t1) == 0) {
    waits_for_.emplace(t1, std::vector<txn_id_t>());
  }
  if (std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) == waits_for_[t1].end()) {
    waits_for_[t1].emplace_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":attempt remove edge" + std::to_string(t1) +
                        "->" + std::to_string(t2);
  LOG_DEBUG("%s", loginfo.c_str());
  if (waits_for_.count(t1) == 0) {
    return;
  }
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> ver_set;
  //  std::string loginfo = "Current edge set:";
  //  LOG_DEBUG("%s", loginfo.c_str());
  //  for (auto &[t1, t2] : GetEdgeList()) {
  //    loginfo = "Edge:" + std::to_string(t1) + "->" + std::to_string(t2);
  //    LOG_DEBUG("%s", loginfo.c_str());
  //  }
  for (auto &[key, set] : waits_for_) {
    ver_set.insert(key);
    // dfs downside sear from behind, so here we put smaller id at back
    std::sort(set.begin(), set.end(), std::greater<>());
    for (auto v : set) {
      ver_set.insert(v);
    }
  }
  std::vector<txn_id_t> vertexes(ver_set.begin(), ver_set.end());
  std::sort(vertexes.begin(), vertexes.end());
  size_t n = vertexes.size();
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> on_stack;
  std::stack<txn_id_t> path;
  for (size_t i = 0; i < n; ++i) {
    txn_id_t v = vertexes[i];
    if (visited.count(v) != 0) {
      continue;
    }
    path.push(i);

    while (!path.empty()) {
      auto top = path.top();
      if (visited.count(top) == 0) {
        visited.insert(top);
        on_stack.insert(top);
      } else {
        on_stack.erase(top);
        path.pop();
        continue;
      }

      // explore back first
      for (const auto &next : waits_for_[top]) {
        if (visited.count(next) == 0) {
          path.push(next);
        } else if (on_stack.count(next) == 1) {
          *txn_id = INVALID_TXN_ID;
          for (auto id : on_stack) {
            *txn_id = std::max(*txn_id, id);
          }
          //          loginfo = "Thread " + std::to_string(pthread_self()) + ":cycle detected";
          //          LOG_DEBUG("%s", loginfo.c_str());
          return true;
        }
      }
    }
  }
  //  loginfo = "Thread " + std::to_string(pthread_self()) + ":cycle not detected";
  //  LOG_DEBUG("%s", loginfo.c_str());
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[ori, dest] : waits_for_) {
    for (txn_id_t des : dest) {
      edges.emplace_back(ori, des);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_.clear();

      if (txn_manager_ == nullptr) {
        continue;
      }

      std::unique_lock table_map_lock(table_lock_map_latch_);
      std::unique_lock row_map_lock(row_lock_map_latch_);

      // handle table locks
      for (auto &[_, queue] : table_lock_map_) {
        std::unique_lock queue_lock(queue->latch_);

        //        std::string loginfo =
        //            "Thread " + std::to_string(pthread_self()) + ":request queue for table:" + std::to_string(_);
        //        LOG_DEBUG("%s", loginfo.c_str());

        for (auto &granted_quest : queue->request_queue_) {
          if (!granted_quest->granted_) {
            continue;
          }
          for (auto &waiting_quest : queue->request_queue_) {
            if (!waiting_quest->granted_) {
              AddEdge(waiting_quest->txn_id_, granted_quest->txn_id_);
            }
          }
        }
      }

      for (auto &[_, queue] : row_lock_map_) {
        //        std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":request queue for row:" +
        //        _.ToString(); LOG_DEBUG("%s", loginfo.c_str());
        std::unique_lock queue_lock(queue->latch_);
        for (auto &granted_quest : queue->request_queue_) {
          if (!granted_quest->granted_) {
            continue;
          }
          for (auto &waiting_quest : queue->request_queue_) {
            if (!waiting_quest->granted_) {
              AddEdge(waiting_quest->txn_id_, granted_quest->txn_id_);
            }
          }
        }
      }
      table_map_lock.unlock();
      row_map_lock.unlock();

      txn_id_t abort_txn = INVALID_TXN_ID;
      while (HasCycle(&abort_txn)) {
        txn_manager_->Abort(txn_manager_->GetTransaction(abort_txn));
        for (auto &[t1, t2] : GetEdgeList()) {
          if (t1 == abort_txn || t2 == abort_txn) {
            RemoveEdge(t1, t2);
          }
        }
      }
    }
  }
}

// return false only in cast that no lock is held. Invalid request will lead to exceptions.
auto LockManager::UpgradeLockTable(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                        ":upgrade table lock:" + std::to_string(oid) + ":->" + LockModeToString(lock_mode);
  LOG_DEBUG("%s", loginfo.c_str());
  std::unique_lock map_lock(table_lock_map_latch_);
  // should never be called
  if (table_lock_map_.count(oid) == 0) {
    return false;
  }
  auto request_queue = table_lock_map_[oid];
  std::unique_lock queue_lock(request_queue->latch_);
  map_lock.unlock();
  auto &request_queue_data = request_queue->request_queue_;
  auto it = request_queue_data.begin();
  const txn_id_t txn_id = txn->GetTransactionId();

  while (it != request_queue_data.end()) {
    if ((*it)->txn_id_ == txn_id && (*it)->oid_ == oid && (*it)->granted_) {
      break;
    }
    ++it;
  }

  // current lock not found, should not happen
  if (it == request_queue_data.end()) {
    return false;
  }

  auto current_lock_mode = (*it)->lock_mode_;
  // release current lock first

  request_queue_data.erase(it);
  txn->LockTxn();
  RemoveTxnTableLockLabel(txn, current_lock_mode, oid);
  txn->UnlockTxn();
  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":current lock released";
  LOG_DEBUG("%s", loginfo.c_str());

  if (!CanLockUpgrade(current_lock_mode, lock_mode)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    queue_lock.unlock();
    request_queue->cv_.notify_all();
    throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
  }

  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    auto upgrading_request =
        std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                     [&](LockRequestRef &request) -> bool { return request->txn_id_ == request_queue->upgrading_; });
    if (!AreLocksCompatible(lock_mode, (*upgrading_request)->lock_mode_)) {
      loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                ":trigger UPGRADE_CONFLICT when upgrading table lock:" + std::to_string(oid);
      LOG_DEBUG("%s", loginfo.c_str());
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      queue_lock.unlock();
      request_queue->cv_.notify_all();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
  }

  // upgrade the lock. The real grant is done by lockxx function.
  request_queue_data.push_front(std::make_shared<LockRequest>(txn_id, lock_mode, oid));
  request_queue->upgrading_ = txn_id;
  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":upgrading tag set when upgrading table lock:" + std::to_string(oid);
  LOG_DEBUG("%s", loginfo.c_str());

  queue_lock.unlock();
  request_queue->cv_.notify_all();

  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid,
                                 const RID &rid) -> bool {
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                        ":upgrade row lock:" + std::to_string(oid) + "." + rid.ToString() + ":->" +
                        LockModeToString(lock_mode);
  std::unique_lock map_lock(row_lock_map_latch_);
  // should never be called indeed
  if (row_lock_map_.count(rid) == 0) {
    return false;
  }
  auto request_queue = row_lock_map_[rid];
  std::unique_lock queue_lock(request_queue->latch_);
  map_lock.unlock();
  auto &request_queue_data = request_queue->request_queue_;
  auto it = request_queue_data.begin();
  const txn_id_t txn_id = txn->GetTransactionId();

  while (it != request_queue_data.end()) {
    if ((*it)->txn_id_ == txn_id && (*it)->rid_ == rid && (*it)->oid_ == oid && (*it)->granted_) {
      break;
    }
    ++it;
  }

  if (it == request_queue_data.end()) {
    return false;
  }

  auto current_lock_mode = (*it)->lock_mode_;
  // release the lock first
  request_queue_data.erase(it);
  txn->LockTxn();
  RemoveTxnRowLockLabel(txn, current_lock_mode, oid, rid);
  txn->UnlockTxn();

  if (!CanLockUpgrade(current_lock_mode, lock_mode)) {
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    queue_lock.unlock();
    request_queue->cv_.notify_all();
    throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
  }

  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
              ":trigger UPGRADE_CONFLICT when upgrading row lock:" + std::to_string(oid) + "." + rid.ToString();
    LOG_DEBUG("%s", loginfo.c_str());
    txn->LockTxn();
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    //    request_queue->upgrading_ = INVALID_TXN_ID;
    queue_lock.unlock();
    request_queue->cv_.notify_all();
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }

  // upgrade the lock
  request_queue_data.push_front(std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid));
  request_queue->upgrading_ = txn_id;
  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":upgrading tag set when upgrading row lock:" + std::to_string(oid) + "." + rid.ToString();
  LOG_DEBUG("%s", loginfo.c_str());

  queue_lock.unlock();
  request_queue->cv_.notify_all();

  return true;
}

auto LockManager::AreLocksCompatible(LockManager::LockMode l1, LockManager::LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return l2 != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
  }
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    return false;
  }
  switch (isolation_level) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn_state == TransactionState::GROWING) {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          return true;
        }
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn_state == TransactionState::GROWING) {
        return true;
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn_state == TransactionState::GROWING) {
        return true;
      }

      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
          return true;
        }
      }
      break;
  }
  return false;
}

auto LockManager::GrantNewLocksIsPossible(LockManager::LockRequestQueue *lock_request_queue, Transaction *txn,
                                          LockRequestRef &cur_request) -> bool {
  //  std::unique_lock queue_lock(lock_request_queue->latch_); // not need, queue latched outside

  const txn_id_t txn_id = txn->GetTransactionId();

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  cur_request = nullptr;

  for (auto &quest : lock_request_queue->request_queue_) {
    if (!quest->granted_ && quest->txn_id_ == txn_id) {
      cur_request = quest;
      break;
    }
  }

  if (cur_request == nullptr) {
    return false;
  }

  bool ahead_flag = true;
  bool upgrading_flag = txn_id == lock_request_queue->upgrading_;
  for (auto &quest : lock_request_queue->request_queue_) {
    if (quest == cur_request) {
      ahead_flag = false;
      continue;
    }

    if (quest->granted_) {
      if (!AreLocksCompatible(cur_request->lock_mode_, quest->lock_mode_)) {
        return false;
      }
    }
    // quest is not granted
    if (upgrading_flag) {
      continue;
    }
    if (ahead_flag || quest->txn_id_ == lock_request_queue->upgrading_) {
      if (!AreLocksCompatible(cur_request->lock_mode_, quest->lock_mode_)) {
        return false;
      }
    }
  }

  return true;
}

auto LockManager::CanLockUpgrade(LockManager::LockMode curr_lock_mode, LockManager::LockMode requested_lock_mode)
    -> bool {
  switch (curr_lock_mode) {
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode != LockMode::INTENTION_SHARED;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
  }
  return false;
}
auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid,
                                              LockManager::LockMode row_lock_mode) -> bool {
  switch (row_lock_mode) {
    case LockMode::SHARED:
      return txn->IsTableIntentionSharedLocked(oid) || txn->IsTableSharedLocked(oid) ||
             txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
             txn->IsTableExclusiveLocked(oid);
    case LockMode::EXCLUSIVE:
      return txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
             txn->IsTableExclusiveLocked(oid);
    default:
      return false;
  }
  return false;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  if (visited.empty()) {
    path.push_back(source_txn);
  }

  if (!path.empty()) {
    txn_id_t cur = path.back();
    if (on_path.count(cur) != 0) {
      *abort_txn_id = *std::max_element(path.begin(), path.end());
      return true;
    }

    if (visited.count(cur) == 0) {
      visited.insert(cur);
      on_path.insert(cur);
    } else {
      path.pop_back();
      on_path.erase(cur);
    }

    for (const auto &next : waits_for_[cur]) {
      if (visited.count(next) == 0 && FindCycle(source_txn, path, on_path, visited, abort_txn_id)) {
        return true;
      }
    }
  }
  return false;
}

// lock operation is not included due to performance consideration, remember to lock the txn outside
void LockManager::AddTxnTableLockLabel(Transaction *txn, LockManager::LockMode lockMode, const table_oid_t &oid) {
  switch (lockMode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

// lock operation is not included due to performance consideration, remember to lock the txn outside
void LockManager::RemoveTxnTableLockLabel(Transaction *txn, LockManager::LockMode lockMode, const table_oid_t &oid) {
  switch (lockMode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

// lock operation is not included due to performance consideration, remember to lock the txn outside
void LockManager::AddTxnRowLockLabel(Transaction *txn, LockManager::LockMode lockMode, const table_oid_t &oid,
                                     const RID &rid) {
  if (lockMode == LockMode::SHARED) {
    auto table_set = txn->GetSharedRowLockSet();
    if (table_set->count(oid) == 0) {
      table_set->emplace(oid, std::unordered_set<RID>());
    }
    (*table_set)[oid].insert(rid);
    return;
  }
  if (lockMode == LockMode::EXCLUSIVE) {
    auto table_set = txn->GetExclusiveRowLockSet();
    if (table_set->count(oid) == 0) {
      table_set->emplace(oid, std::unordered_set<RID>());
    }
    (*table_set)[oid].insert(rid);
    return;
  }
  // otherwise, row intention lock is illegal
}

// lock operation is not included due to performance consideration, remember to lock the txn outside
void LockManager::RemoveTxnRowLockLabel(Transaction *txn, LockManager::LockMode lockMode, const table_oid_t &oid,
                                        const RID &rid) {
  if (lockMode == LockMode::SHARED) {
    auto table_set = txn->GetSharedRowLockSet();
    if (table_set->count(oid) != 0) {
      (*table_set)[oid].erase(rid);
    }
    return;
  }
  if (lockMode == LockMode::EXCLUSIVE) {
    auto table_set = txn->GetExclusiveRowLockSet();
    if (table_set->count(oid) != 0) {
      (*table_set)[oid].erase(rid);
    }
    return;
  }
}

auto LockManager::TableIsAlreadyLocked(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid)
    -> bool {
  if (txn->IsTableSharedLocked(oid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }

  if (txn->IsTableExclusiveLocked(oid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }

  if (txn->IsTableIntentionSharedLocked(oid)) {
    lock_mode = LockMode::INTENTION_SHARED;
    return true;
  }

  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }

  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::RowIsAlreadyLocked(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid,
                                     const RID &rid) -> bool {
  if (txn->IsRowSharedLocked(oid, rid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }

  if (txn->IsRowExclusiveLocked(oid, rid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::LockModeToString(LockMode lockMode) -> std::string {
  switch (lockMode) {
    case LockMode::SHARED:
      return "S";
    case LockMode::EXCLUSIVE:
      return "X";
    case LockMode::INTENTION_SHARED:
      return "IS";
    case LockMode::INTENTION_EXCLUSIVE:
      return "IX";
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SIX";
  }
  return "unknown";
}

}  // namespace bustub
