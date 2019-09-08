/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
#include <cassert>

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetSharedLockSet()->count(rid) == 0);

    Request request{txn->GetTransactionId(), LockMode::SHARED, false};
    if (lock_table_.count(rid) == 0) {
        lock_table_[rid].oldest = txn->GetTransactionId();
        lock_table_[rid].list.push_back(request);
    } else {
        // 如果等待队列中没有排他锁，就不需要检测新老程度了，因为这种情况下共享锁都会被授权
        // 不会出现等待的情况，也就不会死锁
        if (lock_table_[rid].exclusive_cnt != 0 && txn->GetTransactionId() > lock_table_[rid].oldest) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        } else {
            lock_table_[rid].oldest = txn->GetTransactionId();
            lock_table_[rid].list.push_back(request);
        }
    }

    // 通过条件：前面全是已授权的shared请求
    Request *cur = nullptr;
    cv_.wait(lk, [&]() -> bool {

        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it->txn_id != txn->GetTransactionId()) {
                if (it->lock_mode != LockMode::SHARED || it->granted) {
                    return false;
                }
            } else {
                cur = &(*it);
                break;
            }
        }
        return true;
    });

    cur->granted = true;
    txn->GetSharedLockSet()->insert(rid);

    // 条件已经发生了变化，其他共享锁请求有机会获取
    cv_.notify_all();
    return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetExclusiveLockSet()->count(rid) == 0);

    Request request{txn->GetTransactionId(), LockMode::EXCLUSIVE, false};
    if (lock_table_.count(rid) == 0) {
        lock_table_[rid].oldest = txn->GetTransactionId();
        lock_table_[rid].list.push_back(request);
    } else {
        if (txn->GetTransactionId() > lock_table_[rid].oldest) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        } else {
            lock_table_[rid].oldest = txn->GetTransactionId();
            lock_table_[rid].list.push_back(request);
        }
    }
    // 通过条件：当前请求之前没有任何已授权的请求
    Request *cur = nullptr;
    cv_.wait(lk, [&]() -> bool {
        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it->txn_id != txn->GetTransactionId()) {
                if (it->granted) {
                    return false;
                }
            } else {
                cur = &(*it);
                break;
            }
        }
        return true;
    });

    cur->granted = true;
    lock_table_[rid].exclusive_cnt++;
    txn->GetExclusiveLockSet()->insert(rid);

    // 授权一个排它锁后，无论共享锁还是排它锁都不可能有机会获取，所以不需要notify
    return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetSharedLockSet()->count(rid) != 0);

    // 通过条件：想要升级的这个shared请求是唯一一个granted请求
    cv_.wait(lk, [&]() -> bool {
        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it == lock_table_[rid].list.begin() && it->txn_id != txn->GetTransactionId()) {
                return false;
            }
            if (it != lock_table_[rid].list.begin() && it->granted) {
                return false;
            }
        }
        return true;
    });

    auto cur = lock_table_[rid].list.begin();
    cur->lock_mode = LockMode::EXCLUSIVE;
    lock_table_[rid].exclusive_cnt++;
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> latch(mutex_);
    assert(txn->GetSharedLockSet()->count(rid) || txn->GetExclusiveLockSet()->count(rid));

    if (strict_2PL_) {
        if (txn->GetState() != TransactionState::ABORTED ||
                txn->GetState() != TransactionState::COMMITTED) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
    }
    if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
    }

    for (auto it = lock_table_[rid].list.begin();
            it != lock_table_[rid].list.end(); ++it) {
        if (it->txn_id == txn->GetTransactionId()) {
            if (it->lock_mode == LockMode::SHARED) {
                txn->GetSharedLockSet()->erase(rid);
            } else {
                txn->GetExclusiveLockSet()->erase(rid);
                lock_table_[rid].exclusive_cnt--;
            }
            lock_table_[rid].list.erase(it);
            break;
        }
    }
    // 更新oldest
    for (auto it = lock_table_[rid].list.begin();
            it != lock_table_[rid].list.end(); ++it) {
        if (it->txn_id < lock_table_[rid].oldest) {
            lock_table_[rid].oldest = it->txn_id;
        }
    }
    cv_.notify_all();
    return true;
}

} // namespace cmudb