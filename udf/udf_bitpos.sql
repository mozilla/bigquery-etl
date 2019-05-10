CREATE TEMP FUNCTION
  udf_bitpos( bits INT64 ) AS ( CAST(SAFE.LOG(bits & -bits, 2) AS INT64));

/*

Returns a 0-based index of the rightmost set bit in the passed bit pattern
or null if no bits are set (bits = 0).

To determine this position, we take a bitwise AND of the bit pattern and
its complement, then we determine the position of the bit via base-2 logarithm;
see https://stackoverflow.com/a/42747608/1260237

Examples:

SELECT udf_bitpos(0);
null

SELECT udf_bitpos(1);
0

SELECT udf_bitpos(2);
1

SELECT udf_bitpos(8);
3

SELECT udf_bitpos(8 + 1);
0

*/
