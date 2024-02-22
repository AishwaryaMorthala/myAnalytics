# Databricks notebook source
#importing libraries
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

#dbutils.widgets.help()

# COMMAND ----------

#text
dbutils.widgets.text("table1", "")
dbutils.widgets.text("table2", "")
table1=dbutils.widgets.get("table1")
table2=dbutils.widgets.get("table2")

# COMMAND ----------

#Reading tables from ADLS
#df_table1_FL="/mnt/adlstestcontainer/Bronze/2024/29/01/SalesPerson.csv"
#df_table2_FL="/mnt/adlstestcontainer/Bronze/2024/29/01/SalesPersonQuotaHistory.csv"
df_table1=spark.read.csv(table1, header=True, sep=',')
df_table2=spark.read.csv(table2, header=True, sep=',')
#df_table1.show()
#df_table2.show()


# COMMAND ----------

#silver_file.show()

# COMMAND ----------

#joining two tables
df_join = df_table1.join(df_table2, 'BusinessEntityID', "inner").select('BusinessEntityID','TerritoryID','SalesYTD')

# COMMAND ----------

#Adding loadtime column
df_join_columnadd = df_join.withColumn("load_timestamp", current_timestamp())

# COMMAND ----------

#date year month day
date=datetime.now().strftime("%Y_%m_%d")
year = datetime.now().strftime("%Y")  
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

# COMMAND ----------

#saving joined file to silver with date
from datetime import datetime
df_join_columnadd.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ",").csv("/mnt/adlstestcontainer/silver/" + year + "/" + month + "/" + day + "/joined_tables")


# COMMAND ----------

#writing to delta
date=datetime.now().strftime("%Y_%m_%d")
df_join_columnadd.write.format("delta").saveAsTable("sampledb.Jointable"+date)

# COMMAND ----------


