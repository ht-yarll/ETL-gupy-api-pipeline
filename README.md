
# ETL Gupy API

## Goal
To build a robust ETL (Extract, Transform, Load) pipeline that:

Fetches data from an API.
Processes and stores the data in Google Cloud Storage (GCS), preferably in .parquet format for optimized storage and querying.
Loads the data into BigQuery by creating or updating a table.
Enables data analysis and visualization through a dashboard built with Looker Studio (BI tool) and styled using Figma.
Key ETL Steps:

 - **Extract** ⤵️:
Fetch raw data from the API using Python libraries like *requests*.
Implement error handling and logging to ensure data retrieval reliability.

- **Transform** 🔃:
Clean and preprocess the data using *pandas*:
Handle missing or inconsistent values.
Apply schema validation and formatting.
Convert the processed data into .parquet format using *pandas* or *pyarrow* for efficient storage and compression.

- **Load to Cloud Storage** ⤴️🪣:
Upload the .parquet file to a specified Google Cloud Storage bucket using the google-cloud-storage library.
Use versioning or timestamps in filenames for better tracking.

- **Load to BigQuery** ⤴️🔍:
Load data into BigQuery using the google-cloud-bigquery library.
Configure optional parameters like write disposition (WRITE_TRUNCATE or WRITE_APPEND) based on the pipeline's needs.
Visualization and Dashboard:

- **Connect BigQuery to Looker Studio for visualization** 📊👀.
Build and style the dashboard in Looker Studio, leveraging Figma for custom design elements.


## Used Libraries

 - [Pandas 🐼](https://pandas.pydata.org/docs/reference/index.html)
 - [Google BigQuery ☁️🔍](https://cloud.google.com/bigquery/docs)
 - [Google Cloud Storage ☁️🪣](https://cloud.google.com/storage/docs)

## Roadmap

- [X]   **Fetch Data**
- [X]   Transform into .parquet
- [X]   Send to **CloudStorage**
- [X]   Upload to **BigQuery**
- [X]   Work with **LookerStudio** and **Figma** to **design a dashboard**
- [ ]   Post **insights** and **final dashboard**

## Key Takeaways

**Cloud Storage and BigQuery**: Efficient storage and processing of large volumes of data.  

**Data Formats (Parquet)**: Advantages in terms of performance and compression.  

**Dashboard Design**: UX/UI principles applied to dashboards.  

**BI Tools**: Real-time data connection and visualization.

## Author
- [@ht-yarll](https://github.com/ht-yarll)
