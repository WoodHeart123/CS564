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

  int numBlockR = numFrames - 2;
  // BUFFER: tuplesR | tuplesS | tuplesOut
  char *tuplesR = buffer;
  char *tuplesS = buffer + numBlockR * tuplePerPage * tupleSize;
  char *tuplesOut = buffer + (numBlockR + 1) * tuplePerPage * tupleSize;

  // Iterate over R by block
  for (int i = 0; i < numPagesR; i += numBlockR)
  {
    int numBlockPagesR = min(numBlockR, numPagesR - i);
    file.read(tuplesR, i, numBlockPagesR);

    // The end of the currently loaded R blocks
    char *tuplesRBlockEnd = tuplesR + numBlockPagesR * tuplePerPage * tupleSize;

    // Iterate over S
    for (int j = 0; j < numPagesS; j++)
    {
      file.read(tuplesS, numPagesR + j);

      // The end of the currently loaded S block
      char *tuplesSPageEnd = tuplesS + tuplePerPage * tupleSize;
      // The current R tuple being examed
      char *tuplesRBlockPtr = tuplesR;

      // Iterate over tuples until exceeding the addr range
      while (tuplesRBlockPtr < tuplesRBlockEnd)
      {
        const Tuple &tupleR = *reinterpret_cast<Tuple *>(tuplesRBlockPtr);
        // The current S tuple being examed
        char *tuplesSPagePtr = tuplesS;

        while (tuplesSPagePtr < tuplesSPageEnd)
        {
          const Tuple &tupleS = *reinterpret_cast<Tuple *>(tuplesSPagePtr);

          if (tupleR.first == tupleS.first)
          {
            // Write to buffer
            Tuple *resultTuplePtr = reinterpret_cast<Tuple *>(tuplesOut + (numTuplesOut % tuplePerPage) * tupleSize);
            resultTuplePtr->first = tupleR.second;
            resultTuplePtr->second = tupleS.second;
            numTuplesOut++;

            if (numTuplesOut % tuplePerPage == 0)
            {
              file.write(tuplesOut, pageIndexOut);
              pageIndexOut++;
            }
            break;
          }
          tuplesSPagePtr += tupleSize;
        }
        tuplesRBlockPtr += tupleSize;
      }
    }
  }

  // Write any remaining tuples
  if (numTuplesOut % tuplePerPage != 0)
  {
    file.write(tuplesOut, pageIndexOut);
  }

  return numTuplesOut;
}