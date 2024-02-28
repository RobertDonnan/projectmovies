# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType 
from pyspark.sql.functions import col, date_format

# COMMAND ----------

# DBTITLE 1,Mount databricks file
dbutils.fs.mount(
    source='wasbs://project-movies-data@moviesdataset.blob.core.windows.net',
    mount_point='/mnt/project-movies-data',
    extra_configs = {'fs.azure.account.key.moviesdataset.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)


# COMMAND ----------

# DBTITLE 1,Shows content of containers
# MAGIC %fs
# MAGIC ls "/mnt/project-movies-data"

# COMMAND ----------

# DBTITLE 1,Load Read Action.csv
action = spark.read.format("csv").load("/mnt/project-movies-data/raw-data/action.csv")

# COMMAND ----------

# DBTITLE 1,Show Action Tables
action.show()

# COMMAND ----------

# DBTITLE 1,Load Action + others
action = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/action.csv")
adventure = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/adventure.csv")
horror = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/horror.csv")
sifi = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/scifi.csv")
thriller = spark.read.format("csv").option("header","true").load("/mnt/project-movies-data/raw-data/thriller.csv")

# COMMAND ----------

# DBTITLE 1,shows data type and schema
action.printSchema()

# COMMAND ----------

# DBTITLE 1,changing ratings in action to int
action = action.withColumn("rating", col("rating").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Rating has changed to int
action.printSchema()

# COMMAND ----------

# DBTITLE 1,Sets limits to number of movies listed
all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

# DBTITLE 1,Shows action movie list that contain comedies and limits list to 15
comedies = action.filter(col("genre").contains("Comedy")).limit(15).show()

# COMMAND ----------

# DBTITLE 1,adds action folder to transformed-data folder
action.write.option("header", "true").csv("mnt/project-movies-data/transformed-data/action")

# COMMAND ----------

# DBTITLE 1,Changes year to date type
action =action.withColumn("year", date_format(col("year"), "yyyy"))

# COMMAND ----------

# DBTITLE 1,Ordering action movies
all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

# DBTITLE 1,Write's file again with changes
action.write.mode("overwrite").option("header", "true").csv("/mnt/project-movies-data/transformed-data/action")
