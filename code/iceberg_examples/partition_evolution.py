#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, LongType, StringType
import random
import string

# Launch Spark Session with Iceberg dependencies
spark = SparkSession\
            .builder\
            .appName("IcebergPartitionEvolution_DH_Example")\
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type","hadoop") \
            .config("spark.sql.catalog.spark_catalog.type","hive") \
            .getOrCreate()

# Function to generate random string
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Categorical values
category_one_vals = ['A', 'B', 'C', 'D']
category_two_vals = ['X', 'Y', 'Z']

# Generate synthetic data
data = [
    (
        i,
        random_string(12),
        random.choice(category_one_vals),
        random.choice(category_two_vals)
    )
    for i in range(1, 10001)
]

# Define schema
schema = StructType([
    StructField("id", LongType(), False),
    StructField("data", StringType(), False),
    StructField("category_one", StringType(), False),
    StructField("category_two", StringType(), False)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show a sample
df.show(5)

# Remove Iceberg Table
spark.sql("""DROP TABLE IF EXISTS spark_catalog.default.sample PURGE""")

# Create Iceberg Table
spark.sql("""CREATE TABLE spark_catalog.default.sample (
                    id bigint,
                    data string,
                    category_one string,
                    category_two string)
                USING iceberg
                PARTITIONED BY (category_one);
                """)

# Insert Data into Iceberg Table
df.writeTo("spark_catalog.default.sample").using("iceberg").append()

# Partition Evolution
spark.sql("ALTER TABLE spark_catalog.default.sample ADD PARTITION FIELD category_two")

# Check Partitions
spark.sql("""SELECT * FROM spark_catalog.default.sample.partitions""").show()
