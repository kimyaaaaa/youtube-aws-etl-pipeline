import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------
# Get Glue arguments
# -----------------------

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME','COUNTRY','INPUT_PATH']
)

country = args['COUNTRY']
input_path = args['INPUT_PATH']

# -----------------------
# Initialize Spark
# -----------------------

sc=SparkContext()

glueContext=GlueContext(sc)

spark=glueContext.spark_session

job=Job(glueContext)

job.init(
    args['JOB_NAME'],
    args
)

# -----------------------
# Read JSON
# -----------------------

df=spark.read \
.option(
    "multiLine",
    "true"
) \
.json(
    input_path
)

# -----------------------
# Flatten items
# -----------------------

df=df.select(
    explode(
        "items"
    ).alias(
        "item"
    )
)

# -----------------------
# Extract columns
# -----------------------

df=df.select(

    col(
        "item.id"
    ).cast(
        IntegerType()
    ).alias(
        "category_id"
    ),

    col(
        "item.snippet.title"
    ).alias(
        "category_name"
    ),

    input_file_name().alias(
        "source_file"
    )

)

# -----------------------
# Add country directly
# -----------------------

df=df.withColumn(
    "country",
    lit(country)
)

# Remove helper column

df=df.drop(
    "source_file"
)

# -----------------------
# Business deduplication
# -----------------------

df=df.dropDuplicates(
[
    "category_id",
    "country"
]
)

# -----------------------
# Validation
# -----------------------

print("Schema")

df.printSchema()

print("Rows")

print(
    df.count()
)

df.show(
    20,
    False
)

# -----------------------
# Write parquet
# -----------------------

# -----------------------
# Write parquet
# -----------------------

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

df.write \
.mode(
    "overwrite"
) \
.partitionBy(
    "country"
) \
.parquet(
"s3://youtube-etl-project-769044546127-eu-north-1-an/warehouse/dim_category/"
)

job.commit()