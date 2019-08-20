/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  // set current size: 1 for the first invalid key
  SetSize(0);
  // set page id
  SetPageId(page_id);
  // set parent id
  SetParentPageId(parent_id);
  // set next page id
  SetNextPageId(INVALID_PAGE_ID);

  // set max page size, header is 28bytes
  int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) /
             (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i)
  {
    if (comparator(key, array[i].first) <= 0)
    {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  int idx = GetSize() - 1;
  while (idx >= 0 && (comparator(key, array[idx].first) < 0)) {
    array[idx + 1] = array[idx];
    --idx;
  }
  array[idx + 1] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  int size = GetSize() / 2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyHalfFrom(src, size);
  IncreaseSize(-1 * size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0)
  {
    return false;
  }

  int low = 0, high = GetSize() - 1, mid;
  while (low <= high)
  {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0)
    {
      low = mid + 1;
    }
    else if (comparator(key, KeyAt(mid)) < 0)
    {
      high = mid - 1;
    }
    else
    {
      value = array[mid].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0)
  {
    return GetSize();
  }

  int low = 0, high = GetSize() - 1, mid;
  while (low <= high)
  {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0)
    {
      low = mid + 1;
    }
    else if (comparator(key, KeyAt(mid)) < 0)
    {
      high = mid - 1;
    }
    else
    {
      // 删除节点
      memmove(array + mid, array + mid + 1,
              static_cast<size_t>((GetSize() - mid - 1) * sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  auto size = GetSize();
  IncreaseSize(-size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  auto start = GetSize();
  for (int i = 0; i < size; ++i)
  {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(array, array + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));

  recipient->CopyLastFrom(pair);

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());

  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  //本节点在父节点中的key稍微调整
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  memmove(array + 1, array, GetSize() * sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CopyFirstFrom");
  }

  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  
  parent->SetKeyAt(parentIndex, item.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
