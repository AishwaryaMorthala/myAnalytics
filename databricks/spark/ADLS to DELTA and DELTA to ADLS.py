# Databricks notebook source
#creating a db in hive metastore
%sql
create database sampledb

file_location = "/mnt/adlsstorage/employee/Employee.csv"
 
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
display(mydf_file)


# COMMAND ----------

mydf_delta=mydf_file.write.format("delta").saveAsTable("sampledb.Employee")

# COMMAND ----------

file_location = "/mnt/adlsstorage/population/Life Expectancy Data.csv"
 
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)

# Rename columns to remove invalid characters
new_columns = [c.replace(" ", "_").replace("(", "").replace(")", "") for c in mydf_file.columns]
mydf_file = mydf_file.toDF(*new_columns)

display(mydf_file)

mydf_delta=mydf_file.write.format("delta").saveAsTable("sampledb.LED")

# COMMAND ----------

file_location = "/mnt/adlsstorage/weather/weatherAUS.csv"
 
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)

display(mydf_file)

mydf_delta=mydf_file.write.format("delta").saveAsTable("sampledb.Weather")

# COMMAND ----------

#delta tables to adls
mydf_delta= spark.read.format("delta").load("dbfs:/user/hive/warehouse/sampledb.db/weather")
adls_path = "/mnt/adlsstorage/output/weather"
mydf=mydf_delta.write.format("delta").save(adls_path)
