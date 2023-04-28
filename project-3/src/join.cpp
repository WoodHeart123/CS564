#include "join.hpp"

#include <array>
#include <cstdint>
#include <vector>

using namespace std;

JoinAlgorithm getJoinAlgorithm()
{
  return JOIN_ALGORITHM_BNLJ;
  // return JOIN_ALGORITHM_SMJ;
  // return JOIN_ALGORITHM_HJ;
  // throw std::runtime_error("not implemented: getJoinAlgorithm");
};

int join(File &file, int numPagesR, int numPagesS, char *buffer, int numFrames)
{
  const int tuplePerPage = 512;
  const int tupleSize = 8;

  int pageIndexOut = numPagesR + numPagesS;

  // num of tuples in total
  int numTuplesOut = 0;

  int blockSizeR = numFrames - 2;
  // BUFFER: tuplesR | tuplesS | tuplesOut
  char *tuplesR = buffer;
  char *tuplesS = buffer + blockSizeR * tuplePerPage * tupleSize;
  char *tuplesOut = buffer + (blockSizeR + 1) * tuplePerPage * tupleSize;

  // Iterate over R by block
  for (int i = 0; i < numPagesR; i += blockSizeR)
  {
    int numBlockPagesR = min(blockSizeR, numPagesR - i);
    file.read(tuplesR, i, numBlockPagesR);

    // Iterate over S
    for (int j = 0; j < numPagesS; j++)
    {
      file.read(tuplesS, numPagesR + j, 1);

      // Iterate over tuples
      for (int k = 0; k < numBlockPagesR * tuplePerPage; k++)
      {
        const Tuple &tupleR = *reinterpret_cast<Tuple *>(tuplesR + k * tupleSize);

        for (int l = 0; l < tuplePerPage; l++)
        {
          const Tuple &tupleS = *reinterpret_cast<Tuple *>(tuplesS + l * tupleSize);

          if (tupleR.first == tupleS.first)
          {
            Tuple resultTuple(tupleR.second, tupleS.second);
            // Write to buffer
            memcpy(tuplesOut + (numTuplesOut % tuplePerPage) * tupleSize, &resultTuple, tupleSize);
            numTuplesOut++;

            if (numTuplesOut % tuplePerPage == 0)
            {
              file.write(tuplesOut, pageIndexOut, 1);
              pageIndexOut++;
            }
            break;
          }
        }
      }
    }
  }

  // Write any remaining tuples
  if (numTuplesOut % tuplePerPage != 0)
  {
    int numPagesOut = (numTuplesOut % tuplePerPage) / tuplePerPage + ((numTuplesOut % tuplePerPage) % tuplePerPage != 0);
    file.write(tuplesOut, pageIndexOut, numPagesOut);
  }

  return numTuplesOut;
}
