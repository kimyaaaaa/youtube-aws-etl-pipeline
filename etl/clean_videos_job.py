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
    ['JOB_NAME','COUNTRY']
)

country=args['COUNTRY']

input_path=f"s3://youtube-etl-project-769044546127-eu-north-1-an/raw/videos/country={country}/"

output_path=f"s3://youtube-etl-project-769044546127-eu-north-1-an/staging/videos/country={country}/"

# -----------------------
# Initialize Glue/Spark
# -----------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args['JOB_NAME'],
    args
)

# -----------------------
# Explicit schema
# -----------------------

schema = StructType([

    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("channelId", StringType(), True),
    StructField("channelTitle", StringType(), True),
    StructField("categoryId", StringType(), True),
    StructField("trending_date", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("view_count", StringType(), True),
    StructField("likes", StringType(), True),
    StructField("dislikes", StringType(), True),
    StructField("comment_count", StringType(), True),
    StructField("thumbnail_link", StringType(), True),
    StructField("comments_disabled", StringType(), True),
    StructField("ratings_disabled", StringType(), True),
    StructField("description", StringType(), True)

])

# -----------------------
# Read CSV
# -----------------------

df = spark.read \
.option("header","true") \
.option("multiLine","true") \
.option("quote",'"') \
.option("escape",'"') \
.option("mode","PERMISSIVE") \
.option("encoding","UTF-8") \
.option("ignoreLeadingWhiteSpace","true") \
.option("ignoreTrailingWhiteSpace","true") \
.schema(schema) \
.csv(input_path)

# -----------------------
# Recover corrupted IDs
# -----------------------

df = df.withColumn(

    "video_id",

    when(

        col("video_id")=="#NAME?",

        regexp_extract(
            col("thumbnail_link"),
            "/vi/([^/]+)/",
            1
        )

    ).otherwise(
        col("video_id")
    )
)

# -----------------------
# Convert types
# -----------------------

df = df.withColumn(
    "publishedAt",
    to_timestamp(
        "publishedAt"
    )
)

df = df.withColumn(
    "trending_date",
    to_date(
        to_timestamp(
            "trending_date",
            "yyyy-MM-dd'T'HH:mm:ssX"
        )
    )
)

df = df.withColumn(
    "categoryId",
    col("categoryId")
    .cast(IntegerType())
)

df = df.withColumn(
    "view_count",
    col("view_count")
    .cast(LongType())
)

df = df.withColumn(
    "likes",
    col("likes")
    .cast(LongType())
)

df = df.withColumn(
    "dislikes",
    col("dislikes")
    .cast(LongType())
)

df = df.withColumn(
    "comment_count",
    col("comment_count")
    .cast(LongType())
)

df = df.withColumn(
    "comments_disabled",
    col("comments_disabled")
    .cast(BooleanType())
)

df = df.withColumn(
    "ratings_disabled",
    col("ratings_disabled")
    .cast(BooleanType())
)

# -----------------------
# Fill missing values
# -----------------------

df = df.fillna({

    "view_count":0,
    "likes":0,
    "dislikes":0,
    "comment_count":0,
    "tags":"No Tags"

})

# -----------------------
# Extract country
# -----------------------

df = df.withColumn(

    "country",

    regexp_extract(
        input_file_name(),
        "country=([A-Z]+)",
        1
    )
)

# -----------------------
# Add ingestion timestamp
# -----------------------

df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# -----------------------
# Remove invalid rows
# -----------------------

df = df.filter(

    col("video_id").isNotNull()
)

df = df.filter(

    length(
        trim(
            col("video_id")
        )
    ) > 0
)

# -----------------------
# Deduplicate
# -----------------------

df = df.dropDuplicates(

[
    "video_id",
    "trending_date",
    "country"
]

)

# -----------------------
# Validation
# -----------------------

print("Schema")

df.printSchema()

print("Total rows")

print(
    df.count()
)

print("Countries")

df.select(
    "country"
).distinct().show(
    truncate=False
)

print("Remaining #NAME rows")

df.filter(
    col("video_id")=="#NAME?"
).show(
    20,
    truncate=False
)

print("Sample rows")

df.show(
    10,
    truncate=False
)

# -----------------------
# Write parquet
# -----------------------

df.write \
.mode("overwrite") \
.parquet(output_path)

job.commit()