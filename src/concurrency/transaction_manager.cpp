//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  txn->LockTxn();
  auto revert_records = txn->GetWriteSet();
  txn->UnlockTxn();
  std::string loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
                        ":revert changes start";
  LOG_DEBUG("%s", loginfo.c_str());
  // revert from back to front
  for (auto it = revert_records->rbegin(); it != revert_records->rend(); ++it) {
    switch (it->wtype_) {
      case WType::INSERT:
        it->table_heap_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, it->rid_);
        break;
      case WType::DELETE:
        it->table_heap_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, false}, it->rid_);
        break;
      case WType::UPDATE:
        break;
    }
  }
  loginfo = "Thread " + std::to_string(pthread_self()) + ":txn " + std::to_string(txn->GetTransactionId()) +
            ":revert changes complete";
  LOG_DEBUG("%s", loginfo.c_str());

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
