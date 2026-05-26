\# Dataset Setup



\## Download Dataset



Download the YouTube Trending dataset from Kaggle.



Dataset source:



https://www.kaggle.com/datasets/datasnaek/youtube-new



After downloading:



1\. Extract the dataset archive.



2\. Create the following folder structure inside your local project:



```text

raw/



├── videos/

│    ├── country=US/

│    ├── country=CA/

│    ├── country=GB/

│    ├── country=DE/

│    └── country=MX/

│

└── categories/

&#x20;    ├── country=US/

&#x20;    ├── country=CA/

&#x20;    ├── country=GB/

&#x20;    ├── country=DE/

&#x20;    └── country=MX/

```



3\. Move video CSV files into the corresponding folders:



Example:



```text

USvideos.csv

&#x20;   → raw/videos/country=US/



CAvideos.csv

&#x20;   → raw/videos/country=CA/



GBvideos.csv

&#x20;   → raw/videos/country=GB/

```



4\. Move category JSON files:



Example:



```text

US\_category\_id.json

&#x20;   → raw/categories/country=US/



CA\_category\_id.json

&#x20;   → raw/categories/country=CA/

```



Expected final structure:



```text

raw/



├── videos/

│    ├── country=US/

│    │      └── USvideos.csv

│    │

│    ├── country=CA/

│    │      └── CAvideos.csv

│    │

│    └── ...

│

└── categories/

&#x20;    ├── country=US/

&#x20;    │      └── US\_category\_id.json

&#x20;    │

&#x20;    ├── country=CA/

&#x20;    │      └── CA\_category\_id.json

&#x20;    │

&#x20;    └── ...

```



Once the files are placed correctly, upload the `raw` directory contents to the S3 bucket used by the project.



