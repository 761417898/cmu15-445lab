#include <list>

#include "hash/extendible_hash.h"
#include "common/logger.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : I(1), bucketSize(size) {
	dirSize = 1 << I;
	Dir.push_back(0);
	Dir.push_back(1);
	J.push_back(1);
	J.push_back(1);
	std::map<K, V> map;
	buckets.push_back(map);
	buckets.push_back(map);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
	//std::lock_guard<std::mutex> lck (hashMtx);
    return std::hash<K>()(key) % dirSize;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
	std::lock_guard<std::mutex> lck (hashMtx);
	return I;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
	std::lock_guard<std::mutex> lck (hashMtx);
    return J[bucket_id];
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
	std::lock_guard<std::mutex> lck (hashMtx);
    return buckets.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> lck (hashMtx);
  int dirIndex = HashKey(key);
  int bucketIndex = Dir[dirIndex];
  auto iter = buckets[bucketIndex].find(key);
  if (iter == buckets[bucketIndex].end()) {
  	LOG_INFO("Unfind");
  	return false;
  } else {
  	value = iter->second;
  	LOG_INFO("%d bucket Find", bucketIndex);
  	return true;
  }
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> lck (hashMtx);
  int dirIndex = HashKey(key);
  int bucketIndex = Dir[dirIndex];
  return buckets[bucketIndex].erase(key);
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Split(int dirIndex) {
//	std::lock_guard<std::mutex> lck (hashMtx);
	LOG_INFO("bucket Split");
	int bucketIndex = Dir[dirIndex];
	if (J[bucketIndex] == I) {
		buckets.push_back(std::map<K, V>());
		I++;
		dirSize *= 2;
		J[bucketIndex]++;
		J[buckets.size() - 1] = J[bucketIndex];
		for (int idx = 0; idx < dirSize / 2; ++idx) {
			Dir.push_back(Dir[idx]);
		}
		Dir[bucketIndex + dirSize / 2] = buckets.size() - 1;
	} else {
		J[bucketIndex]++;
		buckets.push_back(std::map<K, V>());
		J[buckets.size() - 1] = J[bucketIndex];
		Dir[bucketIndex + dirSize / 2] = buckets.size() - 1;
	}
	//bucketIndex concent rehash
	std::map<K, V> map(buckets[bucketIndex]);
	buckets[bucketIndex].erase(buckets[bucketIndex].begin(), buckets[bucketIndex].end());
	for (auto iter = map.begin(); iter != map.end(); ++iter) {
		auto key = iter->first;
		dirIndex = HashKey(key);
		bucketIndex = Dir[dirIndex];
		buckets[bucketIndex].insert(std::make_pair(key, iter->second));
	}
}

template <typename K, typename V>
void ExtendibleHash<K, V>::InsertAux(const K &key, const V &value) {
	int dirIndex = HashKey(key);
	int bucketIndex = Dir[dirIndex];
	while (buckets[bucketIndex].size() == bucketSize) {
		Split(dirIndex);
		dirIndex = HashKey(key);
		bucketIndex = Dir[dirIndex];
	}
	buckets[bucketIndex].insert(std::make_pair(key, value));
	LOG_INFO("%d bucket Insert", bucketIndex);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
	std::lock_guard<std::mutex> lck (hashMtx);
	InsertAux(key, value);
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
