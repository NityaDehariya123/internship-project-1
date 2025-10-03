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

## Output Screenshots

All project output screenshots are in the my_outputs folder:

![Screenshot 1](my_outputs/Screenshot%202025-10-03%20093557.png)
![Screenshot 2](my_outputs/screenshot2.png)
![Screenshot 3](my_outputs/screenshot3.png)

## Purpose
This project demonstrates how to handle CSV data, perform data cleaning, store it in optimized formats, and integrate with Hive for querying and analytics—showcasing essential *big data engineering skills*.
