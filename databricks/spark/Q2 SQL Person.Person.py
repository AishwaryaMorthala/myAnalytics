# Databricks notebook source
file_location = "/mnt/adlstestcontainer/Person/Person"
 
#read in the data to dataframe df
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#display the dataframe
display(mydf_file)


# COMMAND ----------

from pyspark.sql.functions import *

selected_df = mydf_file.select("BusinessEntityID", "FirstName", "LastName")
filtered_df = selected_df.filter((col("PersonType") == "IN") & (col("LastName") == "Adams"))
sorted_df = filtered_df.sort("FirstName", ascending=True)
with_timestamp_df = sorted_df.withColumn("load_timestamp",current_timestamp())
with_timestamp_df.show(100,False)

# COMMAND ----------

mydf_delta = with_timestamp_df.write.mode("append").format("delta").saveAsTable("sampledb.ListofPersons")

# COMMAND ----------


