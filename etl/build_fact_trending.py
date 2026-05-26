import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------
# Get arguments
# -----------------------

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'COUNTRY'
    ]
)

country = args['COUNTRY']

# -----------------------
# Initialize Spark
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
# Read staging videos
# -----------------------

videos_df = spark.read.parquet(
"s3://youtube-etl-project-769044546127-eu-north-1-an/staging/videos/"
).filter(
    col("country") == country
)

# -----------------------
# Read category dimension
# -----------------------

category_df = spark.read.parquet(
"s3://youtube-etl-project-769044546127-eu-north-1-an/warehouse/dim_category/"
).filter(
    col("country") == country
)

# -----------------------
# Join
# -----------------------

fact_df = videos_df.alias(
    "v"
).join(

    category_df.alias(
        "c"
    ),

    (
        col("v.categoryId")
        ==
        col("c.category_id")
    )
    &
    (
        col("v.country")
        ==
        col("c.country")
    ),

    "left"
)

# -----------------------
# Derived columns
# -----------------------

fact_df = fact_df.withColumn(

    "engagement_rate",

    when(
        col("v.view_count") > 0,

        round(
            (
                col("v.likes")
                +
                col("v.comment_count")
            )
            /
            col("v.view_count"),
            4
        )
    ).otherwise(0)

)

fact_df = fact_df.withColumn(

    "like_ratio",

    when(
        col("v.view_count") > 0,

        round(
            col("v.likes")
            /
            col("v.view_count"),
            4
        )
    ).otherwise(0)

)

fact_df = fact_df.withColumn(

    "published_day",

    date_format(
        col(
            "v.publishedAt"
        ),
        "EEEE"
    )

)

fact_df = fact_df.withColumn(

    "trending_year",

    year(
        col(
            "v.trending_date"
        )
    )

)

# -----------------------
# Select final columns
# -----------------------

fact_df = fact_df.select(

    col("v.video_id"),

    col("v.title"),

    col("v.channelId"),

    col("v.channelTitle"),

    col("v.categoryId"),

    col("c.category_name"),

    col("v.publishedAt"),

    col("v.trending_date"),

    col("v.view_count"),

    col("v.likes"),

    col("v.dislikes"),

    col("v.comment_count"),

    col("v.comments_disabled"),

    col("v.ratings_disabled"),

    col("v.country"),

    col("engagement_rate"),

    col("like_ratio"),

    col("published_day"),

    col("trending_year")

)

# -----------------------
# Deduplicate incoming file
# -----------------------

fact_df = fact_df.dropDuplicates(
[
    "video_id",
    "trending_date",
    "country"
]
)

# -----------------------
# Remove records already
# existing in warehouse
# -----------------------

try:

    existing_df = spark.read.parquet(
"s3://youtube-etl-project-769044546127-eu-north-1-an/warehouse/fact_trending_videos/"
    ).filter(
        col(
            "country"
        ) == country
    ).select(

        "video_id",
        "country",
        "trending_date"

    )

    # Force Spark to access S3
    existing_df.limit(
        1
    ).count()

    print(
        "Warehouse exists. Running anti-join..."
    )

    fact_df = fact_df.join(

        existing_df,

        [
            "video_id",
            "country",
            "trending_date"
        ],

        "left_anti"

    )

    print(
        "Anti-join completed successfully."
    )

except Exception as e:

    print(
        "First load or warehouse does not exist."
    )

    print(
        str(e)
    )

# -----------------------
# Validation
# -----------------------

print("Schema")

fact_df.printSchema()

print("Total Rows")

print(
    fact_df.count()
)

print("Countries")

fact_df.select(
    "country"
).distinct().show(
    truncate=False
)

print("Sample")

fact_df.show(
    20,
    truncate=False
)

# -----------------------
# Write
# -----------------------

fact_df.write \
.mode(
    "append"
) \
.partitionBy(
    "country"
) \
.parquet(
"s3://youtube-etl-project-769044546127-eu-north-1-an/warehouse/fact_trending_videos/"
)

job.commit()