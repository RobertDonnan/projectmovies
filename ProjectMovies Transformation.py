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

# COMMAND ----------

# DBTITLE 1,Read CSV file and create spark table 
#Read CSV file as a Spark DataFrame 

action = spark.read.format("csv")\
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load("/mnt/project-movies-data/raw-data/action.csv") \
                  .createOrReplaceTempView("temp_table")

#Create a Spark table from the temporary view - temp table
spark.sql("CREATE TABLE IF NOT EXISTS actionb USING parquet AS SELECT * FROM temp_table")


# COMMAND ----------

# DBTITLE 1,Query to view movies with a rating of 8
#Query to view movies with a rating of 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actionb WHERE rating = 8")

# COMMAND ----------

# DBTITLE 1,Imports plotly 
# Import Plotly Express for visualisation
import plotly.express as ps

# COMMAND ----------

# DBTITLE 1,Panda dataframe for plotting 
#Create a pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# COMMAND ----------

# DBTITLE 1,Group by year, cout movies and crete a DataFrame with "year" and "count" columns
#Group by year, cout movies and crete a DataFrame with "year" and "count" columns 
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# COMMAND ----------

# DBTITLE 1,Create and display bar chart using plotly 
# Create the bar chart using Plotly
fig = ps.bar(grouped_df, x="year", y="count")
fig.update_layout(width=900, height=600)  # Set plot size
fig.show()  # Display the plot


# COMMAND ----------

# DBTITLE 1,Altered code from above but with colours 
# Read the CSV file as a Spark DataFrame
action = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/project-movies-data/raw-data/action.csv") \
    .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS actiontb USING parquet AS SELECT * FROM temp_table")

# Query for movies with rating = 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actiontb WHERE rating = 8.2")

# Import Plotly Express for visualisation
import plotly.express as px

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by year, count movies, and create a DataFrame with "year" and "count" columns
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# Create the bar chart using Plotly (with color customisation)
fig = px.bar(grouped_df, x="year", y="count", color="count", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()



# COMMAND ----------

# DBTITLE 1,Changing column to Data type from String 
from pyspark.sql.functions import col, date_format
scifi = sifi.withColumn("year", col("year").cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Change's column to an int 
scifi = sifi.withColumn("rating", col("rating").cast(IntegerType()))
sifi.printSchema()

# COMMAND ----------

# DBTITLE 1,Writing the transformed data 
adventure.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/adventure")
horror.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/horror")
thriller.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/thriller")


# COMMAND ----------

# DBTITLE 1,Movies made sorted by year and genre 
action = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/project-movies-data/raw-data/action.csv")

sifi = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/project-movies-data/raw-data/scifi.csv")

adventure = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/project-movies-data/raw-data/adventure.csv")

horror = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/project-movies-data/raw-data/horror.csv")

thriller = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("/mnt/project-movies-data/raw-data/thriller.csv")

# COMMAND ----------

action.createOrReplaceTempView("action_view")
sifi.createOrReplaceTempView("sifi_view")
adventure.createOrReplaceTempView("adventure_view")
horror.createOrReplaceTempView("horror_view")
thriller.createOrReplaceTempView("thriller_view")

# COMMAND ----------

# DBTITLE 1,Combines movie counts by year
query_result = spark.sql("""
SELECT year,
SUM(CASE WHEN genre = 'action' THEN 1 ELSE 0 END) AS action_count, 
SUM(CASE WHEN genre = 'sifi' THEN 1 ELSE 0 END) AS sifi_count, 
SUM(CASE WHEN genre = 'horror' THEN 1 ELSE 0 END) AS horror_count, 
SUM(CASE WHEN genre = 'thriller' THEN 1 ELSE 0 END) AS thriller_count, 
SUM(CASE WHEN genre = 'adventure' THEN 1 ELSE 0 END) AS adventure_count
FROM(
    SELECT year, movie_name, rating, 'action' AS genre
    FROM action_view
    UNION ALL
    SELECT year, movie_name, rating, 'sifi' AS genre
    FROM sifi_view
    UNION ALL
    SELECT year, movie_name, rating, 'adventure' AS genre
    FROM adventure_view
    UNION ALL
    SELECT year, movie_name, rating, 'horror' AS genre
    FROM horror_view
    UNION ALL
    SELECT year, movie_name, rating, 'thriller' AS genre
    FROM thriller_view
) combined
GROUP BY year
ORDER BY year ASC
""")                         

# COMMAND ----------

# DBTITLE 1,Import plotly for visualisation 
import plotly.express as ps 

# COMMAND ----------

# DBTITLE 1,pandas dataframe for plotting 
pandas_df = query_result.toPandas()

# COMMAND ----------

# DBTITLE 1,Creating and displaying plotly graph
#Bar chart using plotly 
fig = ps.bar(pandas_df, x="year", y=["action_count", "sifi_count", "horror_count", "adventure_count", "thriller_count"], barmode="group", color_continuous_scale="Turbo")

#Set y-axis range to 0-200
fig.update_layout(yaxis_range=[0,165])

#Changes barmode from stacked to grouped
fig.update_layout(barmode="group")

#set plot size and other formatting options
fig.update_layout(width=1200, height=800, legend_title="Genre", legend_title_font_size=12)

#Displays plot 
fig.show()

# COMMAND ----------

# DBTITLE 1,New change 

