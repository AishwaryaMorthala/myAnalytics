# Databricks notebook source
file_location1 = "/mnt/adlstestcontainer/Person/Person"
 
#read in the data to dataframe df
mydf_file1 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location1)
 
#display the dataframe
display(mydf_file1)

file_location2 = "/mnt/adlstestcontainer/Person/PersonPhone"
 
#read in the data to dataframe df
mydf_file2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location2)
 
#display the dataframe
display(mydf_file2)

# COMMAND ----------

from pyspark.sql.functions import *
join_df=mydf_file2.join(mydf_file1,"BusinessEntityID","left")
selected_df=join_df.select("BusinessEntityID","FirstName","LastName","PhoneNumber")
filtered_df = selected_df.filter(selected_df.LastName.startswith('L'))
with_timestamp_df = filtered_df.withColumn("load_timestamp",current_timestamp())

# COMMAND ----------

with_timestamp_df.show(10,False)

# COMMAND ----------

mydf_delta = with_timestamp_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("sampledb.personStartL")

# COMMAND ----------


