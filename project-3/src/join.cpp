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

  int numTuplesOut = 0;
  int blockSize = numFrames - 1;
  int numTuplesPerBlock = blockSize * 512;

  std::vector<Tuple> tuplesR(numTuplesPerBlock);
  std::vector<Tuple> tuplesS(512);
  std::vector<Tuple> tuplesOut;

  // Iterate over R by block
  for (int i = 0; i < numPagesR; i += blockSize)
  {
    int blockPagesR = std::min(blockSize, numPagesR - i);
    file.read(tuplesR.data(), pageIndexR + i, blockPagesR);
    int numTuplesR = blockPagesR * 512;

    // Iterate over S
    for (int j = 0; j < numPagesS; j++)
    {
      file.read(tuplesS.data(), pageIndexS + j, 1);

      // Iterate over tuples
      for (int k = 0; k < numTuplesR; k++)
      {
        const Tuple &tupleR = tuplesR[k];

        for (const Tuple &tupleS : tuplesS)
        {
          if (tupleR.first == tupleS.first)
          {
            tuplesOut.emplace_back(tupleR.second, tupleS.second);
            if (tuplesOut.size() == 512)
            {
              file.write(tuplesOut.data(), pageIndexOut, 1);
              pageIndexOut += 1;
              numTuplesOut += 512;
              tuplesOut.clear();
            }
            break;
          }
        }
      }
    }
  }

  // Write any remaining tuples
  if (!tuplesOut.empty())
  {
    int numPagesOut = tuplesOut.size() / 512 + (tuplesOut.size() % 512 != 0);
    file.write(tuplesOut.data(), pageIndexOut, numPagesOut);
    numTuplesOut += tuplesOut.size();
  }

  return numTuplesOut;
}