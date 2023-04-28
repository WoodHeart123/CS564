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

  int pageIndexR = 0;
  int pageIndexS = pageIndexR + numPagesR;
  int pageIndexOut = pageIndexS + numPagesS;

  // num of tuples in total
  int numTuplesOut = 0;
  // num of tuples in buffer waiting to be written
  int numTuplesBuffer = 0;

  int blockSizeR = numFrames - 2;
  // BUFFER: tuplesR | tuplesS | tuplesOut
  char *tuplesR = buffer;
  char *tuplesS = buffer + blockSizeR * tuplePerPage * tupleSize;
  char *tuplesOut = buffer + (blockSizeR + 1) * tuplePerPage * tupleSize;

  // Iterate over R by block
  for (int i = 0; i < numPagesR; i += blockSizeR)
  {
    int blockPagesR = min(blockSizeR, numPagesR - i);
    file.read(tuplesR, pageIndexR + i, blockPagesR);

    for (int j = 0; j < numPagesS; j++)
    {
      file.read(tuplesS, pageIndexS + j, 1);

      // Iterate over S
      for (int k = 0; k < blockPagesR * tuplePerPage; k++)
      {
        const Tuple &tupleR = *reinterpret_cast<Tuple *>(tuplesR + k * tupleSize);

        // Iterate over tuples
        for (int l = 0; l < tuplePerPage; l++)
        {
          const Tuple &tupleS = *reinterpret_cast<Tuple *>(tuplesS + l * tupleSize);

          if (tupleR.first == tupleS.first)
          {
            Tuple resultTuple(tupleR.second, tupleS.second);
            // Write to buffer
            memcpy(tuplesOut + numTuplesBuffer * tupleSize, &resultTuple, tupleSize);
            numTuplesBuffer++;

            if (numTuplesBuffer == tuplePerPage)
            {
              file.write(tuplesOut, pageIndexOut, 1);
              pageIndexOut++;
              numTuplesOut += numTuplesBuffer;
              numTuplesBuffer = 0;
            }
            break;
          }
        }
      }
    }
  }

  // Write any remaining tuples
  if (numTuplesBuffer > 0)
  {
    int numPagesOut = numTuplesBuffer / tuplePerPage + (numTuplesBuffer % tuplePerPage != 0);
    file.write(tuplesOut, pageIndexOut, numPagesOut);
    numTuplesOut += numTuplesBuffer;
    numTuplesBuffer = 0;
  }

  return numTuplesOut;
}