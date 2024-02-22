# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://adlstestcontainer@adlstestdev.blob.core.windows.net/",
    mount_point='/mnt/adlstestcontainer',
    extra_configs={
        "fs.azure.account.key.adlstestdev.blob.core.windows.net": 
            'KEwwMiwtnZx7mLztYjPwIlNK05sS22H8T/1N/UxxqyOf47yJ2JWGX22BqP3mHJIyJVRBhMs4vnKQ+AStBM0f8w=='            
    }
)

# COMMAND ----------

dbutils.fs.ls('/mnt/adlstestcontainer')

# COMMAND ----------

file_location = "/mnt/adlstestcontainer/Production/ProductInventory"
 
#read in the data to dataframe df
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#display the dataframe
display(mydf_file)


# COMMAND ----------

selected_df=mydf_file.groupBy("ProductID").agg(sum("Quantity").alias("TotalQuantity"))
selected_df = mydf_file.join(selected_df, "ProductID")
finalDf=selected_df.filter((col("TotalQuantity") > 500) & ((col("Shelf") == 'A') | (col("Shelf") == 'C') | (col("Shelf") == 'H')))
with_timestamp_df = finalDf.withColumn("load_timestamp",current_timestamp())
with_timestamp_df.show(100,False)


# COMMAND ----------

mydf_delta = with_timestamp_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("sampledb.productquantity")
