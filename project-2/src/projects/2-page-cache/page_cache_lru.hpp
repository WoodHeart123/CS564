#ifndef CS564_PROJECT_PAGE_CACHE_LRU_HPP
#define CS564_PROJECT_PAGE_CACHE_LRU_HPP

#include <unordered_map>
#include <queue>
#include <algorithm>
#include "page_cache.hpp"

class LRUReplacementPageCache : public PageCache {
public:
  LRUReplacementPageCache(int pageSize, int extraSize);

  ~LRUReplacementPageCache();

  void setMaxNumPages(int maxNumPages) override;

  [[nodiscard]] int getNumPages() const override;

  Page *fetchPage(unsigned int pageId, bool allocate) override;

  void unpinPage(Page *page, bool discard) override;

  void changePageId(Page *page, unsigned int newPageId) override;

  void discardPages(unsigned int pageIdLimit) override;

private:
  struct LRUReplacementPage : public Page {
    LRUReplacementPage(int pageSize, int extraSize, unsigned pageId,
                          bool pinned);

    unsigned pageId;
    bool pinned;
  };

  std::unordered_map<unsigned, LRUReplacementPage *> pages_;
  std::vector<unsigned> freePageIDList;
  
};

#endif // CS564_PROJECT_PAGE_CACHE_LRU_HPP
