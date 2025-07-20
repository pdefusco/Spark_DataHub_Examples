# Spark_DataHub_Examples

A collection of Spark examples in Cloudera On Cloud - DataHub platform.

### Requirements

### Instructions

#### PySpark App Simple

Load to /user/user via Hue. Then submit with:

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

Load to /user/user via Hue. Then submit with:

```
curl -X POST https://data-engine-observe-gateway.observe.xfaz-gdb4.cloudera.site/data-engine-observe/cdp-proxy-api/livy_for_spark3/batches \
 -H "Content-Type: application/json" \
 -u pauldefusco:pwd!! \
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

#### Iceberg Partition Evolution

The application is located at `/code/iceberg_examples/partition_evolution.py`. Load to `/user/user` via Hue. Then update spark cluster url, user and password, and submit with:

```
curl -X POST https://spark-cluster-arm-gateway.pdf-jul2.a465-9q4k.cloudera.site/spark-cluster-arm/cdp-proxy-api/livy_for_spark3/batches \
 -H "Content-Type: application/json" \
 -u pauldefusco:pwd! \
 -d '{
  "file": "/user/pauldefusco/partition_evolution.py",
  "name": "PythonSQL",
  "conf": {
   "spark.dynamicAllocation.enabled": "true",
   "spark.dynamicAllocation.minExecutors": "1",
   "spark.dynamicAllocation.maxExecutors": "20"
  },
  "executorMemory": "4g",
  "executorCores": 4
 }'
```
