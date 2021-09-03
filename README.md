# LSH-IS-Spark
Spark implementation of LSH Instance Selection algorithms:
- LSH-IS-S: One instance of each class per bucket is selected.
- LSH-IS-F: If there is only one instance of a class into the bucket, it is removed.

For more information about the algorithms based on hashing:
 **Á. Arnaiz-González, J-F. Díez Pastor, Juan J. Rodríguez, C. García Osorio.** _Instance selection of linear complexity for big data._ Knowledge-Based Systems, 107, 83-95. [doi: 10.1016/j.knosys.2016.05.056](https://doi.org/10.1016/j.knosys.2016.05.056)

# Parameters
The parameters of the algorithm are:
- bucketLength: length of the buckets (numeric, default 1.0).
- numHashTablesAnd: number of AND tables (numeric, default 10).
- numHashTablesOr: number of OR tables (numeric, default 4).
- filter: whether noise filter (LSH-IS-F) is active or not (LSH-IS-S) (boolean, default true).

