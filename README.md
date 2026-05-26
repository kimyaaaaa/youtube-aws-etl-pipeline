\# YouTube AWS ETL Pipeline



End-to-end cloud ETL pipeline for processing and analyzing YouTube trending video data using AWS services, Apache Airflow orchestration, Spark transformations, Athena analytics, and Power BI visualization.



\---



\## Project Overview



This project builds a complete data pipeline that:



\- Detects new files automatically

\- Cleans and transforms raw YouTube data

\- Creates fact and dimension tables

\- Stores data in an S3 data lake

\- Uses Apache Airflow for orchestration

\- Uses AWS Glue (Spark) for ETL jobs

\- Uses Athena for querying

\- Uses Power BI for visualization and analytics



\---



\## Architecture



Pipeline flow:



Raw Data → S3 Raw Zone → AWS Glue ETL → S3 Staging → Warehouse Layer → Athena → Power BI



\---



\## Tech Stack



\- Python

\- Apache Airflow

\- AWS Glue

\- Apache Spark

\- Amazon S3

\- AWS Athena

\- AWS Glue Data Catalog

\- Power BI

\- SQL



\---



\## S3 Data Lake Structure



```text

youtube-etl-project/

│

├── raw/

│   ├── videos/

│   └── categories/

│

├── staging/

│   └── videos/

│

├── warehouse/

│   ├── fact\_trending\_videos/

│   └── dim\_category/

│

├── metadata/

│

└── athena-results/

```



\## Airflow Pipeline



Pipeline tasks:



1\. Detect new files

2\. Clean video data

3\. Build category dimension

4\. Build fact table

5\. Save processed files



DAG workflow:



!\[Airflow DAG](screenshots/airflow-dag.png)



\---



\## AWS Glue Jobs



The project contains three Glue ETL jobs:



\### clean\_videos\_job



\- Reads raw CSV files

\- Cleans invalid values

\- Converts data types

\- Saves parquet output



\### build\_category\_dimension



\- Processes category JSON files

\- Creates dimension table



\### build\_fact\_trending



\- Builds fact table

\- Creates calculated metrics:



&#x20;   - engagement\_rate

&#x20;   - like\_ratio

&#x20;   - trending\_days



\---



\## Athena Analytics Queries



Example business questions:



\### Top channels by deduplicated views in 2023



```sql

WITH latest\_video\_views AS (

&#x20;   SELECT

&#x20;       video\_id,

&#x20;       channelTitle,

&#x20;       MAX(view\_count) AS final\_views

&#x20;   FROM fact\_trending\_videos

&#x20;   WHERE trending\_year = 2023

&#x20;   GROUP BY

&#x20;       video\_id,

&#x20;       channelTitle

)



SELECT

&#x20;   channelTitle,

&#x20;   SUM(final\_views) AS total\_views,

&#x20;   COUNT(video\_id) AS unique\_videos

FROM latest\_video\_views

GROUP BY channelTitle

ORDER BY total\_views DESC

LIMIT 10;

```



Result:



!\[Athena Result](screenshots/query result.png)



\---



\## Dashboard



Power BI dashboard includes:



\- Views by category

\- Trending duration analysis

\- Country comparison

\- Engagement metrics

\- Channel performance

\- Trending videos analysis



Dashboard screenshots available in:



```text

screenshots/

```



Power BI dashboard download:



See Releases section.



\---



\## Dataset



Dataset is excluded from GitHub because of file size.



Download dataset from Kaggle:



https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset



Place files in:



```text

data/raw/

```



Expected structure:



```text

data/raw/

├── US\_youtube\_trending\_data.csv

├── CA\_youtube\_trending\_data.csv

├── FR\_youtube\_trending\_data.csv

├── MX\_youtube\_trending\_data.csv

├── US\_category\_id.json

├── CA\_category\_id.json

├── FR\_category\_id.json

└── MX\_category\_id.json

```



\---



\## Setup



Clone repository:



```bash

git clone https://github.com/kimyaaaaa/youtube-aws-etl-pipeline.git

```



Install dependencies:



```bash

pip install -r requirements.txt

```



Run Airflow:



```bash

airflow standalone

```



\---

