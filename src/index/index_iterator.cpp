/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() { }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* page, 
	size_t index, BufferPoolManager* buffer_pool_manager) {
	page_ = page;
	index_ = index;
	buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator const& index_iterator) {
	page_ = index_iterator.page_;
	index_ = index_iterator.index_;
	buffer_pool_manager_ = index_iterator.buffer_pool_manager_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
	buffer_pool_manager_->UnpinPage(page_->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
	return page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
	if (index_ < page_->GetSize() - 1) {
		++index_;
		return *this;
	}
	if (page_->GetNextPageId() == INVALID_PAGE_ID) {
		index_ = -1;
		return *this;
	}
	auto next_page_id = page_->GetNextPageId();
	buffer_pool_manager_->UnpinPage(page_->GetPageId(), true);
	auto page = buffer_pool_manager_->FetchPage(next_page_id);
    page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
    index_ = 0;
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
	return index_ == -1;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
