#include "page_cache_lru_2.hpp"
#include "utilities/exception.hpp"
#include<iostream>

LRU2ReplacementPageCache::LRU2ReplacementPage::LRU2ReplacementPage(
    int argPageSize, int argExtraSize, unsigned argPageId, bool argPinned, unsigned argPinCount)
    : Page(argPageSize, argExtraSize), pageId(argPageId), pinned(argPinned), pinCount(argPinCount) {}

LRU2ReplacementPageCache::LRU2ReplacementPageCache(int pageSize, int extraSize)
    : PageCache(pageSize, extraSize) {}
    
LRU2ReplacementPageCache::~LRU2ReplacementPageCache() {
  for (auto &[pageId, page] : pages_) {
    free(page);
  }
}

void LRU2ReplacementPageCache::setMaxNumPages(int maxNumPages) {
  maxNumPages_ = maxNumPages;

  // try evict in the second place of free list
  while(!freePageIDListOne.empty() && getNumPages() > maxNumPages_){
    free(pages_[freePageIDListOne[0]]);
    pages_.erase(freePageIDListOne[0]);
    freePageIDListOne.erase(freePageIDListOne.begin());
  } 

  // evict last unpinned page if set exceeds max num pages
  while(freePageIDList.size() > 1 && getNumPages() > maxNumPages_){
    free(pages_[freePageIDList[1]]);
    pages_.erase(freePageIDList[1]);
    freePageIDList.erase(freePageIDList.begin() + 1);
  }
  if(freePageIDList.size() == 1 && getNumPages() > maxNumPages_){
    free(pages_[freePageIDList[0]]);
    pages_.erase(freePageIDList[0]);
    freePageIDList.erase(freePageIDList.begin());
  }
}

int LRU2ReplacementPageCache::getNumPages() const {
  return (int)pages_.size();
}

Page *LRU2ReplacementPageCache::fetchPage(unsigned pageId, bool allocate) {
  ++numFetches_;

  // If the page is already in the cache, pin it and return the pointer.
  auto pagesIterator = pages_.find(pageId);
  if (pagesIterator != pages_.end()) {
    ++numHits_;
    // remove from free list
    pagesIterator->second->pinned = true;
    pagesIterator->second->pinCount++;
    return pagesIterator->second;
  }

  // The page is not already in the cache. If parameter `allocate` is false,
  // return a null pointer.
  if (!allocate) {
    return nullptr;
  }
  // since hit is O(1), remove pinned page during miss
  auto freeListIterator = freePageIDListOne.begin();
  while (freeListIterator != freePageIDListOne.end()) {
    if (pages_[*freeListIterator] -> pinned || pages_[*freeListIterator] -> pinCount > 1){
      freeListIterator = freePageIDListOne.erase(freeListIterator);
    }
    else {
      ++freeListIterator;
    }
  }
  freeListIterator = freePageIDList.begin();
  while (freeListIterator != freePageIDList.end()) {
    // if page pinned, remove it
    if (pages_[*freeListIterator] -> pinned) freeListIterator = freePageIDList.erase(freeListIterator);
    // remove duplicates in vector
    else if(std::find(freeListIterator+1, freePageIDList.end(), *freeListIterator) != freePageIDList.end()) freeListIterator = freePageIDList.erase(freeListIterator);
    else ++freeListIterator;
  }

  // Parameter `allocate` is true. If the number of pages in the cache is less
  // than the maximum, allocate and return a pointer to a new page.
  if (getNumPages() < maxNumPages_) {
    auto page = new LRU2ReplacementPage(pageSize_, extraSize_, pageId, true, 1);
    pages_.emplace(pageId, page);
    return page;
  }

  int unpinnedPageID = -1;
  // if unpinned page with one access exist
  if(!freePageIDListOne.empty()){
    unpinnedPageID = freePageIDListOne[0];
    freePageIDListOne.erase(freePageIDListOne.begin());
  }else if(freePageIDList.size() == 1){
    unpinnedPageID = freePageIDList[0];
    freePageIDList.erase(freePageIDList.begin());
  }else if(freePageIDList.size() > 1){
    unpinnedPageID = freePageIDList[1];
    freePageIDList.erase(freePageIDList.begin() + 1);
  }
  if(unpinnedPageID != -1){
    LRU2ReplacementPage *newPage =  pages_[unpinnedPageID];
    pages_.erase(pages_.find(unpinnedPageID));
    pages_.emplace(pageId, newPage);
    newPage->pinCount = 1;
    return newPage;
  }
  // All pages are pinned. Return a null pointer.
  return nullptr;
}

void LRU2ReplacementPageCache::unpinPage(Page *page, bool discard) {
  auto *discardPage = (LRU2ReplacementPage *) page;

  // If discard is true or the number of pages in the cache is greater than the
  // maximum, discard the page. Otherwise, unpin the page.
  if (discard || getNumPages() > maxNumPages_) {
    pages_.erase(discardPage->pageId);
    delete page;
  } else {
    discardPage->pinned = false;
    if(discardPage->pinCount == 1){
      std::cout << "page " << discardPage->pageId << " push to one access list" << std::endl;
      freePageIDListOne.push_back(discardPage->pageId);
    }else{
      std::cout << "page " << discardPage->pageId <<  "push to more than one access list" << std::endl;
      freePageIDList.push_back(discardPage->pageId);
    }
    
  }
}

void LRU2ReplacementPageCache::changePageId(Page *page, unsigned newPageId) {
  auto *newPage = (LRU2ReplacementPage *) page;
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
    return;
  }
  // replace it from free list
  std::replace(freePageIDList.begin(), freePageIDList.end(), oldPageId, newPageId);
}

void LRU2ReplacementPageCache::discardPages(unsigned pageIdLimit) {
  for (auto pagesIterator = pages_.begin(); pagesIterator != pages_.end();) {
    if (pagesIterator->second->pageId >= pageIdLimit) {
      // remove from free list
      std::vector<unsigned>::iterator position = std::find(freePageIDList.begin(), freePageIDList.end(), pagesIterator->second->pageId);
      if (position != freePageIDList.end())
        freePageIDList.erase(position);
      // delete from map
      delete pagesIterator->second;
      pagesIterator = pages_.erase(pagesIterator);
    } else {
      ++pagesIterator;
    }
  }
}
