#include "join.hpp"

#include <array>
#include <cstdint>
#include <vector>

JoinAlgorithm getJoinAlgorithm()
{
  return JOIN_ALGORITHM_BNLJ;
  // return JOIN_ALGORITHM_SMJ;
  // return JOIN_ALGORITHM_HJ;
  // throw std::runtime_error("not implemented: getJoinAlgorithm");
};

int join(File &file, int numPagesR, int numPagesS, char *buffer, int numFrames)
{
  int pageIndexR = 0;
  int pageIndexS = pageIndexR + numPagesR;
  int pageIndexOut = pageIndexS + numPagesS;
  int blockSize = numFrames * PAGE_SIZE;
  int bufferIndex = 0;

  int numBlocksR = (int)ceil((double)numPagesR * PAGE_SIZE / blockSize);
  int numBlocksS = (int)ceil((double)numPagesS * PAGE_SIZE / blockSize);

  std::vector<Tuple> tuplesOut;

  for (int i = 0; i < numBlocksR; i++)
  {
    int numPagesRBlock = (i == numBlocksR - 1) ? numPagesR % (blockSize / PAGE_SIZE) : blockSize / PAGE_SIZE;
    int pageIndexRBlock = pageIndexR + i * numPagesRBlock;

    for (int j = 0; j < numBlocksS; j++)
    {
      int numPagesSBlock = (j == numBlocksS - 1) ? numPagesS % (blockSize / PAGE_SIZE) : blockSize / PAGE_SIZE;
      int pageIndexSBlock = pageIndexS + j * numPagesSBlock;

      file.read(buffer + bufferIndex, pageIndexRBlock, numPagesRBlock);
      std::vector<Tuple> tuplesR((Tuple *)(buffer + bufferIndex), (Tuple *)(buffer + bufferIndex + numPagesRBlock * PAGE_SIZE));
      bufferIndex += numPagesRBlock * PAGE_SIZE;

      file.read(buffer + bufferIndex, pageIndexSBlock, numPagesSBlock);
      std::vector<Tuple> tuplesS((Tuple *)(buffer + bufferIndex), (Tuple *)(buffer + bufferIndex + numPagesSBlock * PAGE_SIZE));
      bufferIndex += numPagesSBlock * PAGE_SIZE;

      for (const Tuple &tupleR : tuplesR)
      {
        for (const Tuple &tupleS : tuplesS)
        {
          if (tupleR.first == tupleS.first)
          {
            tuplesOut.emplace_back(tupleR.second, tupleS.second);
          }
        }
      }

      if (bufferIndex + blockSize > numFrames * PAGE_SIZE)
      {
        int numTuplesOut = (int)tuplesOut.size();
        int numPagesOut = numTuplesOut / (PAGE_SIZE / sizeof(Tuple)) + (numTuplesOut % (PAGE_SIZE / sizeof(Tuple)) != 0);

        file.write(buffer, pageIndexOut, numPagesOut);
        tuplesOut.clear();
        bufferIndex = 0;
      }
    }
  }

  int numTuplesOut = (int)tuplesOut.size();
  int numPagesOut = numTuplesOut / (PAGE_SIZE / sizeof(Tuple)) + (numTuplesOut % (PAGE_SIZE / sizeof(Tuple)) != 0);
  file.write(buffer, pageIndexOut, numPagesOut);

  return numTuplesOut;
}
