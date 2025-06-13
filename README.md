# Spark_DataHub_Examples

A collection of Spark examples in Cloudera On Cloud - DataHub platform.

### Requirements

### Instructions

#### PySpark App Simple

The SparkSubmit via Livy Endpoint API:

```
curl -u "pauldefusco" \
  -X POST "https://prd-cluster-master0.rapids-d.a465-9q4k.cloudera.site/prd-cluster/cdp-proxy-api/livy_for_spark3/batches" \
  -H "Content-Type: application/json" \
  -d "{
    "file": "code/pyspark-app-simple/pyspark-simple.py",
    "name": "SimplePySparkApp"
    }"
```
