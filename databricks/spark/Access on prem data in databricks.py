# Databricks notebook source
import pyodbc

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DESKTOP-0T4U7M9;'
                      'Database=AdventureWorks2022;'
                      'Trusted_Connection=yes;')

# COMMAND ----------

df = pd.read_sql('SELECT * FROM myTable', conn)

# COMMAND ----------

import pyodbc
drivers=pyodbc.drivers()
print(drivers)

# COMMAND ----------

query = "SELECT [BusinessEntityID],[FirstName],[LastName],
                 [PostalCode],[City] FROM [Sales].[vSalesPerson]"
df = pd.read_sql(query, sql_conn)

df.head(3)
