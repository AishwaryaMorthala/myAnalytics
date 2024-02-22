# Databricks notebook source
#set the data lake file location:
file_location = "/mnt/adlsstorage/employee/Employee.csv"
 
#read in the data to dataframe df
mydf_file = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#display the dataframe
display(mydf_file)

# COMMAND ----------

type(mydf_file)

# COMMAND ----------

mydf_file.show()

# COMMAND ----------

from pyspark.sql.functions import *
mydf_file1 = mydf_file.withColumn("JoiningDate", concat(lit("01-01-"), col("JoiningYear"))) 

# COMMAND ----------

mydf_file1.show(4,False)

# COMMAND ----------

mydf_file1.show(10,False)

# COMMAND ----------

#creating temporary view
mydf_file1.createOrReplaceTempView("mydf_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from mydf_test

# COMMAND ----------

#UNION on 2 data
mydf_file2=mydf_file1
mydf_union=mydf_file1.union(mydf_file2)
mydf_union.show(4,False)

# COMMAND ----------

# Perform unionAll on selected columns
mydf_union2 = mydf_file1.select("Education","JoiningYear","City","PaymentTier","Age","Gender","EverBenched","ExperienceInCurrentDomain","LeaveOrNot","JoiningDate").unionAll(mydf_file2.select("Education","JoiningYear","City","PaymentTier","Age","Gender","EverBenched","ExperienceInCurrentDomain","LeaveOrNot","JoiningDate"))
mydf_union2.show(4,False)
mydf_union2.count()

# COMMAND ----------

#mydf_union2.write.csv("/mnt/adlsstorage/output/Employeeoutput.csv").overWrite

# COMMAND ----------


