#ifndef CS564_PROJECT_PAGE_CACHE_LRU_2_HPP
#define CS564_PROJECT_PAGE_CACHE_LRU_2_HPP

#include "page_cache.hpp"
#include <unordered_map>
#include <queue>
#include <algorithm>

class LRU2ReplacementPageCache : public PageCache {
public:
  LRU2ReplacementPageCache(int pageSize, int extraSize);

  ~LRU2ReplacementPageCache();

  void setMaxNumPages(int maxNumPages) override;

  [[nodiscard]] int getNumPages() const override;

  Page *fetchPage(unsigned int pageId, bool allocate) override;

  void unpinPage(Page *page, bool discard) override;

  void changePageId(Page *page, unsigned int newPageId) override;

  void discardPages(unsigned int pageIdLimit) override;

private:
    struct LRU2ReplacementPage : public Page {
    LRU2ReplacementPage(int pageSize, int extraSize, unsigned pageId,
                          bool pinned, unsigned pinCount);

    unsigned pageId;
    bool pinned;
    unsigned pinCount;
  };

  std::unordered_map<unsigned, LRU2ReplacementPage *> pages_;
  std::vector<unsigned> freePageIDListOne;
  std::vector<unsigned> freePageIDList;
};

#endif // CS564_PROJECT_PAGE_CACHE_LRU_2_HPP
