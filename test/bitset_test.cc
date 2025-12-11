#include <gtest/gtest.h>

#include "common/Type.h"
#include "transaction/Bitset.h"

class BitsetTest : public ::testing::Test {
 public:
  void CheckSingleIter(Bitset::SingleIterator& s_iter, ColumnId cid) {
    ASSERT_TRUE(s_iter.Valid());
    ASSERT_EQ(s_iter.Value(), cid);
    s_iter.Next();
  }

  void CheckRangeIter(Bitset::RangeIterator& r_iter, ColumnId s_cid, ColumnId e_cid) {
    ColumnId start_cid, end_cid;
    ASSERT_TRUE(r_iter.Valid());
    r_iter.Value(&start_cid, &end_cid);
    ASSERT_EQ(start_cid, s_cid);
    ASSERT_EQ(end_cid, e_cid);
  }
};

TEST_F(BitsetTest, SingleIteratorTest) {
  Bitset bs;
  bs.Add((ColumnId)2);
  bs.Add((ColumnId)7);
  bs.Add((ColumnId)11);
  bs.Add((ColumnId)15);
  bs.Add((ColumnId)16);
  bs.Add((ColumnId)63);

  auto s_iter = bs.Iter();

  CheckSingleIter(s_iter, 2);
  CheckSingleIter(s_iter, 7);
  CheckSingleIter(s_iter, 11);
  CheckSingleIter(s_iter, 15);
  CheckSingleIter(s_iter, 16);
  CheckSingleIter(s_iter, 63);

  ASSERT_FALSE(s_iter.Valid());
}

TEST_F(BitsetTest, RangeIteratorTest) {
  Bitset bs;
  bs.Add((ColumnId)2);
  bs.Add((ColumnId)3);
  bs.Add((ColumnId)4);

  bs.Add((ColumnId)11);
  bs.Add((ColumnId)15);
  bs.Add((ColumnId)16);

  bs.Add((ColumnId)43);
  bs.Add((ColumnId)50);
  bs.Add((ColumnId)63);

  auto r_iter = bs.RangeIter();
  ColumnId start, end;

  CheckRangeIter(r_iter, 2, 4);
  CheckRangeIter(r_iter, 11, 11);
  CheckRangeIter(r_iter, 15, 16);
  CheckRangeIter(r_iter, 43, 43);
  CheckRangeIter(r_iter, 50, 50);
  CheckRangeIter(r_iter, 63, 63);

  ASSERT_FALSE(r_iter.Valid());
}
