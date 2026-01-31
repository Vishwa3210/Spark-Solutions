from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df_result = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/datasets/customers.csv')

# Chain filters together
df_result = df_result.filter(df_result.purchase_amount > 100.00).filter(df_result.age >= 30)
df_result=df_result.select('customer_id','name','purchase_amount')
display(df_result)