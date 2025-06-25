# Spark_DataHub_Examples

A collection of Spark examples in Cloudera On Cloud - DataHub platform.

### Requirements

### Instructions

#### PySpark App Simple

The SparkSubmit via Livy Endpoint API:

```
curl -u "pauldefusco" \
  -X POST "https://spark-cluster-master0.rapids-d.a465-9q4k.cloudera.site/spark-cluster/cdp-proxy-api/livy_for_spark3//batches" \
  -H "Content-Type: application/json" \
  -d "{
    "file": "code/pyspark-app-simple/pyspark-simple.py",
    "name": "SimplePySparkApp"
    }"
```

#### PySpark SQL Simple Query

Submit with:

```
curl -X POST https://data-engine-observe-gateway.observe.xfaz-gdb4.cloudera.site/data-engine-observe/cdp-proxy-api/livy_for_spark3/batches \
 -H "Content-Type: application/json" \
 -u pauldefusco:pwd! \
 -d '{
  "file": "/user/pauldefusco/simple-spark-sql-query.py",
  "name": "SimpleSparkSQLApp",
  "conf": {
   "spark.dynamicAllocation.enabled": "true",
   "spark.dynamicAllocation.minExecutors": "1",
   "spark.dynamicAllocation.maxExecutors": "20"
  },
  "executorMemory": "4g",
  "executorCores": 4
 }'
```
