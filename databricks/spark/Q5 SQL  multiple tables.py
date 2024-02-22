# Databricks notebook source
#Sample table: Person.Person
#Sample table: HumanResources.Employee
#Sample table: Person.Address
#Sample table: Person.BusinessEntityAddress


file_location1 = "/mnt/adlstestcontainer/Person/Person"
mydf_file1 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location1).limit(1000)
display(mydf_file1)
file_location2 = "/mnt/adlstestcontainer/HumanResources/Employee"
mydf_file2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location2).limit(1000)
display(mydf_file2)
file_location3 = "/mnt/adlstestcontainer/Person/Address"
mydf_file3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location3).limit(1000)
display(mydf_file3)
file_location4 = "/mnt/adlstestcontainer/Person/BusinessEntityAddress"
mydf_file4 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location4).limit(1000)
display(mydf_file4)

# COMMAND ----------

from pyspark.sql.functions import *
join1_df = mydf_file3.join(mydf_file4, mydf_file3.AddressID == mydf_file4.AddressID, "inner").join(mydf_file1,mydf_file1.BusinessEntityID == mydf_file1.BusinessEntityID, "inner")


# COMMAND ----------

from pyspark.sql.functions import *
selected_df=join1_df.select("FirstName","LastName","City").distinct()

# COMMAND ----------

sorted_df=selected_df.sort("LastName","FirstName")

# COMMAND ----------

with_timestamp_df = sorted_df.withColumn("load_timestamp",current_timestamp())


# COMMAND ----------

#append
mydf_delta = with_timestamp_df.write.mode("overwrite").format("delta").saveAsTable("sampledb.EmployeeCity")#append everytime the data entered

# COMMAND ----------

mydf_file1.count()#19972
mydf_file3.count()#19614
mydf_file4.count()#19614

# COMMAND ----------


