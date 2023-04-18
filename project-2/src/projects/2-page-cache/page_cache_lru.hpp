#ifndef CS564_PROJECT_PAGE_CACHE_LRU_HPP
#define CS564_PROJECT_PAGE_CACHE_LRU_HPP

#include <unordered_map>
#include <queue>
#include <algorithm>
#include "page_cache.hpp"

class LRUReplacementPageCache : public PageCache
{
public:
  /**
   * Construct a LRU Replacement Page Cache.
   * @param pageSize Page size in bytes.
   * @param extraSize Extra space in bytes.
   */
  LRUReplacementPageCache(int pageSize, int extraSize);

  /**
   * Destroy the LRU Replacement PageCache.
   */
  ~LRUReplacementPageCache();

  /**
   * Set the maximum number of pages in the cache.
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
   * Else if 'allocate' is true and there is avaliable cache space, allocate and
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
  struct LRUReplacementPage : public Page
  {
    /**
     * Construct an LRUReplacementPage object.
     * @param pageSize Page size in bytes.
     * @param extraSize Extra space in bytes.
     * @param pageId Page id.
     * @param pinned Page is pinned.
     */
    LRUReplacementPage(int pageSize, int extraSize, unsigned pageId,
                       bool pinned);

    unsigned pageId;
    bool pinned;
  };

  std::unordered_map<unsigned, LRUReplacementPage *> pages_;
  std::vector<unsigned> freePageIDList;
};

#endif // CS564_PROJECT_PAGE_CACHE_LRU_HPP
