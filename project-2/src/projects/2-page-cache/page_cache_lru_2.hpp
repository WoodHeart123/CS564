#ifndef CS564_PROJECT_PAGE_CACHE_LRU_2_HPP
#define CS564_PROJECT_PAGE_CACHE_LRU_2_HPP

#include "page_cache.hpp"
#include <unordered_map>
#include <queue>
#include <algorithm>

class LRU2ReplacementPageCache : public PageCache
{
public:
  /**
   * Construct an LRU2ReplacementPageCache object.
   * @param pageSize Size in bytes of the page.
   * @param extraSize Size in bytes of the buffer to store extra information.
   */
  LRU2ReplacementPageCache(int pageSize, int extraSize);

  /**
   * Destructor for LRU2 Replacement PageCache.
   */
  ~LRU2ReplacementPageCache();

  /**
   * Set the maximum number of pages in the cache.
   * If existing cache exceeds the new limit, evict using LRU2
   * @param maxNumPages Maximum number of pages in the cache.
   */
  void setMaxNumPages(int maxNumPages) override;

  /**
   * Get the number of pages in the cache.
   * @return Number of pages in the cache.
   */
  [[nodiscard]] int getNumPages() const override;

  /**
   * Fetch and pin the page with the given pageId and return it's pointer.
   * If the page is not in the cache and 'allocate' is false, return null pointer.
   * Else if 'allocate' is true and there is avaliable cache space using LRU2, allocate and
   * return the pointer.
   * @param pageId Page id of the requested page.
   * @param allocate Flag to specify if a new page should be allocated if the requested page doesn't exist.
   * @return Pointer to the requested Page object or nullptr if allocate is false and the page doesn't exist.
   */
  Page *fetchPage(unsigned int pageId, bool allocate) override;

  /**
   * Unpin the given page and discard it if 'discard' is true or pages exceed cache capacity
   * @param page Pointer to the Page object to unpin and possibly discard.
   * @param discard Flag to specify if the page should be discarded.
   */
  void unpinPage(Page *page, bool discard) override;

  /**
   * Change the pageId of the given page.
   * @param page Pointer to the Page object whose pageId is to be changed.
   * @param newPageId New pageId to be set for the page.
   */
  void changePageId(Page *page, unsigned int newPageId) override;

  /**
   * Discard all pages in the cache with pageIds greater than or
   * equal to the given pageIdLimit.
   * @param pageIdLimit lower bound for pageIds to be discarded.
   */
  void discardPages(unsigned int pageIdLimit) override;

private:
  struct LRU2ReplacementPage : public Page
  {
    /**
     * Construct an LRU2ReplacementPage object.
     * @param pageSize Page size in bytes.
     * @param extraSize Extra space in bytes.
     * @param pageId Page id.
     * @param pinned Page is pinned.
     */
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
