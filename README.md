# LSH-IS-Spark
Spark implementation of LSH Instance Selection algorithms:
- LSH-IS-S: One instance of each class per bucket is selected.
- LSH-IS-F: If there is only one instance of a class into the bucket, it is removed (this behaves like a noise filter).

For more information about the algorithms based on hashing:
 **Á. Arnaiz-González, J-F. Díez Pastor, Juan J. Rodríguez, C. García Osorio.** _Instance selection of linear complexity for big data._ Knowledge-Based Systems, 107, 83-95. [doi: 10.1016/j.knosys.2016.05.056](https://doi.org/10.1016/j.knosys.2016.05.056)

# Parameters
The parameters of the algorithm are:
- ```bucketLength```: length of the buckets (numeric, default 1.0).
- ```numHashTablesAnd```: number of AND tables (numeric, default 10).
- ```numHashTablesOr```: number of OR tables (numeric, default 4).
- ```filter```: whether noise filter (LSH-IS-F) is active or not (LSH-IS-S) (boolean, default true).

# Examples

Filtering a dataset by using LSH-IS-F.

```bash
sbt package
spark-shell --jars target/scala-2.12/lshis_2.12-0.0.1.jar
```

```scala
// import
import org.apache.spark.ml.instance.LSHIS

val df = spark.read.format("libsvm").load(".....").persist
val lshis = new LSHIS()
  .setBucketLength(1)
  .setNumHashTablesAnd(10)
  .setNumHashTablesOr(4)
  .setFilter(true)
  .setSeed(54)
lshis.transform(df).show
```
