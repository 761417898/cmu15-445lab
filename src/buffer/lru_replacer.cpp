/**
 * LRU implementation
 */

#include <list>

#include "buffer/lru_replacer.h"
#include "page/page.h"
#include "common/logger.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
	std::lock_guard<std::mutex> lck (mtx_);
	for (auto iter = vec_.begin(); iter != vec_.end(); ++iter) {
		if (*iter == value) {
			vec_.erase(iter);
			break;
		}
	}
	vec_.push_front(value);
	//int x=vec_.size();
	//LOG_INFO("insert, size = %d",x);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
	std::lock_guard<std::mutex> lck (mtx_);
	//LOG_INFO("Victim");
	if (vec_.empty()) {
		return false;
	}
	for (auto iter = vec_.begin(); iter != vec_.end(); ++iter) {
		value = *iter;
	}
	vec_.pop_back();
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
	std::lock_guard<std::mutex> lck (mtx_);
	//LOG_INFO("Erase");
	for (auto iter = vec_.begin(); iter != vec_.end(); ++iter) {
		if (*iter == value) {
			vec_.erase(iter);
			return true;
		}
	}
    return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { 
	std::lock_guard<std::mutex> lck (mtx_);
	//LOG_INFO("Size");
	return vec_.size(); 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
