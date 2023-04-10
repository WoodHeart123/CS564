#include "page_cache_lru.hpp"
#include "utilities/exception.hpp"
#include <iostream>

LRUReplacementPageCache::LRUReplacementPage::LRUReplacementPage(
    int argPageSize, int argExtraSize, unsigned argPageId, bool argPinned)
    : Page(argPageSize, argExtraSize), pageId(argPageId), pinned(argPinned) {}

LRUReplacementPageCache::LRUReplacementPageCache(int pageSize, int extraSize)
    : PageCache(pageSize, extraSize) {}

LRUReplacementPageCache::~LRUReplacementPageCache() {
  for (auto &[pageId, page] : pages_) {
    free(page);
  }
}

void LRUReplacementPageCache::setMaxNumPages(int maxNumPages) {
  maxNumPages_ = maxNumPages;

  // try evict front of free list
  while(!freePageIDList.empty() && getNumPages() > maxNumPages_){
    delete pages_[freePageIDList[0]];
    pages_.erase(freePageIDList[0]);
    freePageIDList.erase(freePageIDList.begin());
  } 
}

int LRUReplacementPageCache::getNumPages() const {
  return (int)pages_.size();
}

Page *LRUReplacementPageCache::fetchPage(unsigned pageId, bool allocate) {
  ++numFetches_;

  // If the page is already in the cache, pin it and return the pointer.
  auto pagesIterator = pages_.find(pageId);
  if (pagesIterator != pages_.end()) {
    ++numHits_;
    pagesIterator->second->pinned = true;
    return pagesIterator->second;
  }

  // The page is not already in the cache. If parameter `allocate` is false,
  // return a null pointer.
  if (!allocate) {
    return nullptr;
  }

  auto freeListIterator = freePageIDList.begin();
  while (freeListIterator != freePageIDList.end()) {
    if (pages_[*freeListIterator] -> pinned) freeListIterator = freePageIDList.erase(freeListIterator);
    else if(std::find(freeListIterator+1, freePageIDList.end(), *freeListIterator) != freePageIDList.end()) freeListIterator = freePageIDList.erase(freeListIterator);
    else ++freeListIterator;
  }

  // Parameter `allocate` is true. If the number of pages in the cache is less
  // than the maximum, allocate and return a pointer to a new page.
  if (getNumPages() < maxNumPages_) {
    auto page = new LRUReplacementPage(pageSize_, extraSize_, pageId, true);
    pages_.emplace(pageId, page);
    return page;
  }
  // return existing unpinned page if there is an available page
  if(!freePageIDList.empty()){
    unsigned unpinnedPageID = freePageIDList[0];
    LRUReplacementPage *newPage = pages_[unpinnedPageID];
    freePageIDList.erase(freePageIDList.begin());
    pages_.erase(unpinnedPageID);
    pages_.emplace(pageId, newPage);
    return newPage;
  }

  // All pages are pinned. Return a null pointer.
  return nullptr;
}

void LRUReplacementPageCache::unpinPage(Page *page, bool discard) {
  auto *discardPage = (LRUReplacementPage *) page;
  // If discard is true or the number of pages in the cache is greater than the
  // maximum, discard the page. Otherwise, unpin the page.
  if (discard || getNumPages() > maxNumPages_) {
    pages_.erase(discardPage->pageId);
    delete page;
  } else {
    discardPage->pinned = false;
    freePageIDList.push_back(discardPage->pageId);
  }
}

void LRUReplacementPageCache::changePageId(Page *page, unsigned newPageId) {
  auto *newPage = (LRUReplacementPage *) page;
  unsigned oldPageId = newPage -> pageId;
  // Remove the old page ID from `pages_` and change the page ID.
  pages_.erase(newPage->pageId);
  newPage->pageId = newPageId;

  // Attempt to insert a page with page ID `newPageId` into `pages_`.
  auto [pagesIterator, success] = pages_.emplace(newPageId, newPage);

  // If a page with page ID `newPageId` is already in the cache, discard it.
  if (!success) {
    delete pagesIterator->second;
    pagesIterator->second = newPage;
    std::vector<unsigned>::iterator position = std::find(freePageIDList.begin(), freePageIDList.end(), oldPageId);
    if (position != freePageIDList.end()) freePageIDList.erase(position);
  }else{
    // if successfully inserted, replace it from free list
    std::replace(freePageIDList.begin(), freePageIDList.end(), oldPageId, newPageId);
  }
  
}

void LRUReplacementPageCache::discardPages(unsigned pageIdLimit) {
    for (auto pagesIterator = pages_.begin(); pagesIterator != pages_.end();) {
      if (pagesIterator->second->pageId >= pageIdLimit) {
        // remove from free list
        if(!pagesIterator->second->pinned){
          std::vector<unsigned>::iterator position = std::find(freePageIDList.begin(), freePageIDList.end(), pagesIterator->second->pageId);
          if (position != freePageIDList.end()) freePageIDList.erase(position);
        }
        // delete from map
        delete pagesIterator->second;
        pagesIterator = pages_.erase(pagesIterator);
      } else {
        ++pagesIterator;
      }
  }
}
