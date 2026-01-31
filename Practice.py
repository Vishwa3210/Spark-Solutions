# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Load the file
df_result = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/datasets/customers_raw.csv')

# REASSIGN each transformation
df_result = df_result.dropna()
# Display the final DataFrame
display(df_result)