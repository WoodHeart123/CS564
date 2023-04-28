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

  // num of tuples in total
  int numTuplesOut = 0;
  // num of tuples in buffer waiting to be written
  int numTuplesBuffer = 0;
  int blockSize = numFrames - 1;

  // BUFFER: tuplesR | tuplesS | tuplesOut
  char *tuplesR = buffer;
  char *tuplesS = buffer + blockSize * 4096;
  char *tuplesOut = buffer + blockSize * 4096 + 4096;

  int tupleSize = 8;

  // Iterate over R by block
  for (int i = 0; i < numPagesR; i += blockSize)
  {
    int blockPagesR = std::min(blockSize, numPagesR - i);
    file.read(tuplesR, pageIndexR + i, blockPagesR);
    int numTuplesR = blockPagesR * 512;

    // Iterate over S
    for (int j = 0; j < numPagesS; j++)
    {
      file.read(tuplesS, pageIndexS + j, 1);

      // Iterate over tuples
      for (int k = 0; k < blockPagesR * 512; k++)
      {
        const Tuple &tupleR = *reinterpret_cast<Tuple *>(tuplesR + k * tupleSize);

        for (int l = 0; l < 512; l++)
        {
          const Tuple &tupleS = *reinterpret_cast<Tuple *>(tuplesS + l * tupleSize);

          if (tupleR.first == tupleS.first)
          {
            Tuple resultTuple(tupleR.second, tupleS.second);
            std::memcpy(tuplesOut + numTuplesOut * 8, &resultTuple, tupleSize);
            numTuplesBuffer++;

            if (numTuplesBuffer == 512)
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
    int numPagesOut = numTuplesBuffer / 512 + (numTuplesBuffer % 512 != 0);
    file.write(tuplesOut, pageIndexOut, numPagesOut);
    numTuplesOut += numTuplesBuffer;
    numTuplesBuffer = 0;
  }

  return numTuplesOut;
}