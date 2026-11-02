# Judul Project
Analyzing Business Trends with Kibana
## Repository Outline
- Folder `images` contain screenshot for all business plot and insights
- P2M3_fauzan_abdurrochman_DAG.py | Contain the DAG (Pipeline) for airflow, starting from make a table for SQL until post data to ElasticSearch
- P2M3_fauzan_abdurrochman_GX.ipynb | Notebook contain a data expectation validation, to ensure data has a high quality
- P2M3_fauzan_abdurrochman_raw.csv | CSV file containing the raw dataset e.g no processing performed
- P2M3_fauzan_abdurrochman_clean.csv | CSV file that has been cleaned (Column normalization, Missing values handling, etc.)

## Problem Background
Restaurant A (Anonymized name) has a problem with its data, each outlet has a different data, meaning data is scattered and unorganized, this is one of the few challenges about outlets, which makes it difficult to analyze business trends & compare performace across outlets

## Project Output
This project produce a dashboard that helps business owner and marketing team to get a better plan for future business strategy, and analyzing business pattern.

## Data
[Mendeley Data](https://data.mendeley.com/datasets/kcgf45y24m/2), this link contains dataset about transactions data on a real-life restaurant over a period of 9 months, containing 11 column, around 50,000 data with no missing values

## Method

This project uses an ETL (Extract, Transform, Load) pipeline to prepare the data, and data visualization using kibana

- Extract: Load the raw transaction data (CSV) into a PostgreSQL database.
- Transform: Clean the data, for example by removing duplicates, normalizing column names, and handling missing values.
- Load: Index the cleaned data into Elasticsearch.
- Visualize: Build a Kibana dashboard to analyze business patterns and compare performance across outlets.

The pipeline was ran using Apache Airflow and visualization using Kibana

## Stacks
Tech Stacks:
- Python
- Apache Airflow for pipeline and scheduling
- ElasticSearch for sending data
- Kibana for Visualization
- Great Expectations for data validation


## Reference
[Data role in modern F&B Business](https://tantri.id/post/peran-data-penting-dalam-bisnis-fb-modern)

---

**Referensi tambahan:**
- [Basic Writing and Syntax on Markdown](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax)
- [Contoh readme](https://github.com/fahmimnalfrzki/Swift-XRT-Automation)
- [Another example](https://github.com/sanggusti/final_bangkit) (**Must read**)
- [Additional reference](https://www.freecodecamp.org/news/how-to-write-a-good-readme-file/)