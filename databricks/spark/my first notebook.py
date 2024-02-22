# Databricks notebook source
my_List = [1, 2, 3, 4, 5]

# COMMAND ----------

my_List_rdd=sc.parallelize(my_List)

# COMMAND ----------

my_List_rdd

# COMMAND ----------

type(my_List_rdd)

# COMMAND ----------

my_List_rdd.collect()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema of the dataframe
my_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Define the data of the dataframe
my_data = [(1, "John Doe", 30), (2, "Jane Smith", 25), (3, "Bob Johnson", 40)]
my_data2= [(1, "Karthik", 30), (2, "eshwar", 25), (3, "spr", 40)]

# Create the dataframe
my_df = spark.createDataFrame(my_data, schema=my_schema)
my_df1= spark.createDataFrame(my_data2, schema=my_schema)

# Show the dataframe
my_df.show()


# COMMAND ----------

my_df.show(10,False)

# COMMAND ----------

my_df1.show(10,False)

# COMMAND ----------

# Join two dataframes - df1 and df2 on column 'key'
join_df = my_df.join(my_df1, 'id')


# COMMAND ----------

join_df.show(10,False)

# COMMAND ----------

# Joining my_df and my_df1 with 'id' as the joining column using inner join
join_df = my_df.join(my_df1,'id','outer')


# COMMAND ----------

# Displaying the first 10 rows of the joined DataFrame without truncating the columns
join_df.show(10,False)


# COMMAND ----------

my_df2=my_df1.where(my_df1.name=='Karthik')

# COMMAND ----------

my_df2.show(10,False)

# COMMAND ----------

from pyspark.sql.functions import col

# Rename columns in first dataframe
my_df = my_df.select(*(col(name).alias('my_df_' + name) for name in my_df.columns))

# Rename columns in second dataframe
my_df1 = my_df1.select(*(col(name).alias('my_df1_' + name) for name in my_df1.columns))

# Join and select column from first dataframe
my_df.join(my_df1, col('my_df_name_my_df') == col('my_df1_name_my_df1')) \
    .select(col('my_df_name_my_df')).show(2)


# COMMAND ----------

joining two tables with column aliases

# COMMAND ----------

from pyspark.sql.functions import col

# Rename columns in first dataframe
my_df = my_df.select(*(col(name).alias('my_df_' + name) for name in my_df.columns))

# Rename columns in second dataframe
my_df1 = my_df1.select(*(col(name).alias('my_df1_' + name) for name in my_df1.columns))

# Join and select column from first dataframe with alias
my_df.join(my_df1, col('my_df_name_my_df') == col('my_df1_name_my_df1')) \
    .select(col('my_df_name_my_df').alias('name_my_df')).show(2)


# COMMAND ----------

my_df.show()
my_df1.show()

# COMMAND ----------

my_df=my_df.withColumnRenamed("Name","name2").withColumnRenamed("age","age2")

finalDf=my_df1.join(my_df, on="id", how="inner")

# COMMAND ----------

finalDf.show(10,False)

# COMMAND ----------

from pyspark.sql.functions import lit

kk=my_df.withColumn("Name", lit(my_df.name2)).drop("name2")

# COMMAND ----------

kk.show(10,False)

# COMMAND ----------

my_df.withColumn("Name", lit(my_df.name2)).dropDuplicates

# COMMAND ----------


