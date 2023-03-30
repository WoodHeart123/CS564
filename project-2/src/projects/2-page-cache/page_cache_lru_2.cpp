#include "page_cache_lru_2.hpp"
#include "utilities/exception.hpp"

LRU2ReplacementPageCache::LRU2ReplacementPage::LRU2ReplacementPage(
    int argPageSize, int argExtraSize, unsigned argPageId, bool argPinned)
    : Page(argPageSize, argExtraSize), pageId(argPageId), pinned(argPinned) {}

LRU2ReplacementPageCache::LRU2ReplacementPageCache(int pageSize, int extraSize)
    : PageCache(pageSize, extraSize) {}

void LRU2ReplacementPageCache::setMaxNumPages(int maxNumPages) {
  maxNumPages_ = maxNumPages;

  // try evict in the second place of free list
  while(freePageIDList.size() > 1 && getNumPages() > maxNumPages_){
    free(pages_[freePageIDList[1]]);
    pages_.erase(freePageIDList[1]);
    freePageIDList.erase(freePageIDList.begin() + 1);
  } 

  // evict last unpinned page if set exceeds max num pages
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
    std::vector<unsigned>::iterator position = std::find(freePageIDList.begin(), freePageIDList.end(), pageId);
    if (position != freePageIDList.end())
      freePageIDList.erase(position);
    }
    pagesIterator->second->pinned = true;
    return pagesIterator->second;
  }

  // The page is not already in the cache. If parameter `allocate` is false,
  // return a null pointer.
  if (!allocate) {
    return nullptr;
  }

  // Parameter `allocate` is true. If the number of pages in the cache is less
  // than the maximum, allocate and return a pointer to a new page.
  if (getNumPages() < maxNumPages_) {
    auto page = new LRUReplacementPage(pageSize_, extraSize_, pageId, true);
    pages_.emplace(pageId, page);
    return page;
  }


  // return existing unpinned page if there is an available page
  if(freePageIDList.size > 1){
    int unpinnedPageID; = freePageIDList[1];
    freePageIDList.erase(freePageIDList.begin()+1);
    pages_[unpinnedPageID] -> pinned = true;
    return pages_[unpinnedPageID];
  }else if(freePageIDList.size == 1){
    int unpinnedPageID; = freePageIDList[0];
    freePageIDList.erase(freePageIDList.begin());
    pages_[unpinnedPageID] -> pinned = true;
    return pages_[unpinnedPageID];
  }
  

  // All pages are pinned. Return a null pointer.
  return nullptr;
}

void LRU2ReplacementPageCache::unpinPage(Page *page, bool discard) {
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

void LRU2ReplacementPageCache::changePageId(Page *page, unsigned newPageId) {
  auto *newPage = (LRUReplacementPage *) page;

  // Remove the old page ID from `pages_` and change the page ID.
  pages_.erase(newPage->pageId);
  // remove it from free list
  if(!newPage->pinned){
    std::vector<unsigned>::iterator position = std::find(freePageIDList.begin(), freePageIDList.end(), newPage->pageId);
    if (position != freePageIDList.end())
      freePageIDList.erase(position);
    }
  
  newPage->pageId = newPageId;

  // Attempt to insert a page with page ID `newPageId` into `pages_`.
  auto [pagesIterator, success] = pages_.emplace(newPageId, newPage);

  // If a page with page ID `newPageId` is already in the cache, discard it.
  if (!success) {
    delete pagesIterator->second;
    pagesIterator->second = newPage;
  }
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
