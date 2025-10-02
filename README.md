# internship-project-1
End-to-end data pipeline using Apache Spark (Scala) to read, clean, and store CSV data in Parquet format on HDFS, create temporary views, and integrate with Hive for querying and analytics.
# CSV Data Processing and Hive Integration using Apache Spark

## Description
An end-to-end data processing pipeline using *Apache Spark (Scala), **HDFS, and **Hive*.  
The workflow includes:  

- Reading a CSV file into a Spark DataFrame with schema inference.  
- Cleaning column names by removing special characters.  
- Saving the cleaned DataFrame in *Parquet* format to HDFS.  
- Reading the Parquet data back into a DataFrame and validating record counts.  
- Creating a temporary view to query data using *Spark SQL*.  
- Creating an external Hive table and verifying records using both Hive CLI and Spark SQL.  

## Technologies Used
- Apache Spark (Scala)  
- HDFS  
- Hive  
- Parquet format  
- Spark SQL & DataFrame API  

## Project Notebook / Output
The full code and output of this project can be viewed [here](./outputs/CSV_Data_Processing_Spark.html)

## Purpose
This project demonstrates how to handle CSV data, perform data cleaning, store it in optimized formats, and integrate with Hive for querying and analytics—showcasing essential *big data engineering skills*.
