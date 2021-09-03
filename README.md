# LSH-IS-Spark
Spark implementation of LSH Instance Selection algorithms:
- LSH-IS-S: One instance of each class per bucket is selected.
- LSH-IS-F: If there is only one instance of a class into the bucket, it is removed.

# Parameters
The parameters of the algorithm are:
- bucketLength: length of the buckets (numeric, default 1.0).
- numHashTablesAnd: number of AND tables (numeric, default 10).
- numHashTablesOr: number of OR tables (numeric, default 4).
- filter: whether noise filter (LSH-IS-F) is active or not (LSH-IS-S) (boolean, default true).

