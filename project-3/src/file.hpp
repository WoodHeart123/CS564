#ifndef CS564_PROJECT_3_FILE_HPP
#define CS564_PROJECT_3_FILE_HPP

#include "extern/sqlite3.h"

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#define PAGE_SIZE 4096

class File {
public:
  explicit File(const std::string &name);

  /**
   * Read pages from a file.
   * @param buffer Read pages into this buffer.
   * @param pageIndex Index of the starting page.
   * @param numPages Number of pages to read.
   */
  void read(void *buffer, int pageIndex, int numPages = 1);

  /**
   * Write pages to a file.
   * @param buffer Write pages from this buffer.
   * @param pageIndex Index of the starting page.
   * @param numPages Number of pages to write.
   */
  void write(void *buffer, int pageIndex, int numPages = 1);

  [[nodiscard]] int getNumReads() const;

  [[nodiscard]] int getNumWrites() const;

private:
  sqlite3_file *file_;

  int numReads_;
  int numWrites_;
};

#endif // CS564_PROJECT_3_FILE_HPP
