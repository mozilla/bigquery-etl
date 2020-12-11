# stats

Statistics functions.


## mode_last_retain_nulls (UDF)

Returns the most frequently occuring element in an array. In the case
of multiple values tied for the highest count, it returns the
value that appears latest in the array. Nulls are retained.
See also: `stats.mode_last, which ignores
nulls.




## mode_last (UDF)

Returns the most frequently occuring element in an array.

In the case of multiple values tied for the highest count, it returns
the value that appears latest in the array. Nulls are ignored.
See also: `stats.mode_last_retain_nulls`,
which retains nulls.


