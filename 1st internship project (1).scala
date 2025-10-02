// Databricks notebook source


// COMMAND ----------

/*
1.read a CSV file into a DataFrame using Scala and find the count (using DataFrame API).
2.clean the DataFrame, remove special characters from column name if, present.
3. save the DataFrame into HDFS path. Tamp Spark Assignment, your name in Perquet format. 
4. make another DF by reading the above HDFS path.
5. find the number of records in the DataFrame (using DataFrame API).
6. store the DataFrame as a view and find the number of records (using Spark SQL API).
7. make an external table on Hive and find the number of records (using BlineClient). 
8. now find number of records of the newly created Hive external table (using Spark SQL API).
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("CSV READER").getOrCreate()

val df = spark.read.option("header" , "true").option("inferSchema" , "true").csv("dbfs:/FileStore/tables/airtravel-1.csv")
val cleanDF = df.toDF(df.columns.map(colName => colName.replaceAll("[^a-zA-Z0-9]","_")): _*)
val hdfsPath = "/tmp/spark-assignment_<Nitya>"
cleanDF.write.mode("overwrite").parquet(hdfsPath)
println(s"Dataframe successfully saved to HDFS at. $hdfsPath")
val newDF = spark.read.format("parquet").load(hdfsPath)
newDF.show(5)
newDF.printSchema()
val newCount = newDF.count()
val recordCount = newDF.count()
println(s"Number of records in the dataframe : $recordCount")
println(s"Number of rows in the new dataframe: $newCount")
val count = df.count()
println("cleared column Names:"+ cleanDF.columns.mkString(","))
println(s"Number of rows: $count")
 
newDF.createOrReplaceTempView("employee_data")
val sqlCountDF = spark.sql("SELECT COUNT(*) AS total_records FROM employee_data")
sqlCountDF.show()
// Register the Parquet file as an external table in Hive
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS employee_data (
        Month STRING,
        Year_1958 DOUBLE,
        Year_1959 DOUBLE,
        Year_1960 DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfsPath '
""")
val countDF = spark.sql("SELECT COUNT(*) AS total_records FROM employee_data")
countDF.show()