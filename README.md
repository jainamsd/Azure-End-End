# Azure-End-End

# End-to-End Azure Data Engineering Project

This project demonstrates a complete **end-to-end data engineering solution** using **Azure Data Lake Storage Gen2**, **Azure Databricks**, and **Delta Lake**. The solution involves creating a multi-layer architecture (Bronze, Silver, Gold) for data processing and transformation, with secure access via Azure AD passthrough.

## Project Overview

In this project, we:
- Ingest raw data from an on-premise SQL Server into the **Bronze Layer** (Raw Data).
- Perform data transformations (e.g., date formatting, column renaming) using PySpark in **Azure Databricks** to move data from Bronze to **Silver** and from Silver to **Gold** layers.
- Use **Delta Lake** format to enable schema evolution and efficient data querying.
- Securely mount **Azure Data Lake Storage Gen2** containers using Azure Active Directory passthrough.
  
## Architecture Overview

- **Bronze Layer**: Raw data, ingested from on-premise SQL Server into ADLS Gen2 using Azure Data Factory.
- **Silver Layer**: Cleaned and transformed data, stored in Delta Lake format.
- **Gold Layer**: Curated and aggregated data, ready for analytics.

## Technologies Used

- **Azure Data Factory** for data ingestion.
- **Azure Data Lake Storage Gen2** for data storage.
- **Azure Databricks** for data transformation.
- **Delta Lake** for optimized data storage and schema evolution.
- **Azure Active Directory (AAD)** and **Azure Key Vault** for secure authentication and governance.

## Mount ADLS Gen2 Containers

The project securely mounts the following layers:

1. **Bronze Layer**:
   ```python
   configs = {
     "fs.azure.account.auth.type": "CustomAccessToken",
     "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
   }

   dbutils.fs.mount(
     source = "abfss://bronze@madhurg.dfs.core.windows.net/",
     mount_point = "/mnt/bronze",
     extra_configs = configs
   )
   dbutils.fs.ls("/mnt/bronze/Sales")
   ```
   2. **Silver Layer**:
      ```python
      dbutils.fs.mount(
      source = "abfss://silver@madhurg.dfs.core.windows.net/",
      mount_point = "/mnt/silver",
      extra_configs = configs)

      ```
   4. **Gold Layer**:
      ```python
      dbutils.fs.mount(
      source = "abfss://gold@madhurg.dfs.core.windows.net/",
      mount_point = "/mnt/gold",
      extra_configs = configs)
      ```

  ## Bronze to Silver Transformation
  This part of the project involves reading raw data from the bronze layer, transforming it, and writing it into the silver layer in Delta format.

  1. **Ingest Data from Bronze Layer**:
     ```python
     df = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/bronze/Sales/Customer/Customer.txt")
     ```
  2. **Apply Date Transformations**:
      ```python
      from pyspark.sql.functions import from_utc_timestamp, date_format
      from pyspark.sql.types import TimestampType
      df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
      ```
  3. **Write to Silver Layer**:
     ```python
     df.write.format('delta').mode('overwrite').save('/mnt/silver/Sales/Customer/')
     ```
## Silver to Gold Transformation
The next phase involves reading the transformed data from the silver layer, renaming columns, and writing the final curated data to the gold layer.

1. **Read Data from Silver Layer**:
   ```python
   df = spark.read.format('delta').option("header", "true").option("inferSchema", "true").load("/mnt/silver/Sales/Customer/")
   ```
2. **Rename Columns to Snake Case**:
   ```python
   for old_col_name in df.columns:
   new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
   df = df.withColumnRenamed(old_col_name, new_col_name)
   ```
3. **Write to Gold Layer**:
   ```python
   df.write.format('delta').mode("overwrite").save('/mnt/gold/Sales/Customer/')
   ```
   
## Conclusion
This project demonstrates how to build an efficient and scalable data pipeline using Azure Databricks, Delta Lake, and Azure Data Lake Storage Gen2. The code automates the transformation and standardization of data from bronze to silver and gold layers, making the data ready for analytics and business intelligence use cases.
