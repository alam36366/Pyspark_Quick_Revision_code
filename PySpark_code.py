from pyspark.sql.window import Window
from pyspark.sql.functions import *
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, ArrayType, TimestampType

spark = SparkSession.builder.appName("PySparkExamples").getOrCreate()

# # Sample data creation
# data = [
#     (1, {"first_name": "Berrie","last_name": "Manueau", "Age": 38}, "bmanueau0@dion.ne.jp", "2006-04-20", "Sports", "F", 154864, 4),
#     (2, {"first_name": "Aeriell","last_name": "McNee", "Age": 35}, "amcnee1@google.es", "2009-01-26", "Tools", "F", 56752, 3),
#     (3, {"first_name": "Sydney","last_name": "Symonds", "Age": 32}, "ssymonds2@hhs.gov", "2010-05-17", "Clothing", "F", 95313, 4),
#     (4, {"first_name": "Avrom","last_name": "Rowantree", "Age": 30}, None, "2014-08-02", "Phones & Tablets", "M", 119674, 7),
#     (5, {"first_name": "Feliks","last_name": "Morffew", "Age": 45}, "fmorffew4@a8.net", "2003-01-14", "Computers", "M", 55307, 5)
# ]
# schema = StructType([
#     StructField("employee_id", IntegerType(), True),
#     StructField("Full_Name & Age", StructType([  # Nested column
#         StructField("first_name", StringType(), True),
#         StructField("last_name", StringType(), True),
#         StructField("Age", IntegerType(), True)
#     ]), True),
#     StructField("email", StringType(), True),
#     StructField("hire_date", StringType(), True),
#     StructField("department", StringType(), True),
#     StructField("gender", StringType(), True),
#     StructField("salary", IntegerType(), True),
#     StructField("region_id", IntegerType(), True)
# ])
# df = spark.createDataFrame(data, schema)

df = spark.read.option('header','true').csv('employees.csv', inferSchema=True)
print(df.count())
df = df.withColumn('Load_time', current_timestamp().cast(StringType()))
df.cache().show(10,False)

# Basic operations
df.select("first_name", "department", "salary").filter(col('salary') > 150000).show(10, False)

# Handling missing data
print(df.na.drop().count())
print("\nFilling NaN values with 0:")
df.withColumn("email", when(col("email")=='NULL', 'Unknown').otherwise(col("email"))).show(10, False)

# Removing spceial charecters 
regex_pattern = "[^a-zA-Z0-9 ]"  # This pattern retains alphanumeric characters and spaces only
df.select([regexp_replace(col(c), regex_pattern, "").alias(c) if dict(df.dtypes)[c] == 'string' else col(c) for c in df.columns]).show(10, False)

# Descriptive statistics
df.describe().show()
# Standard Deviation measures how much individual salaries deviate from the average salary.
stddev_salary = df.select(stddev("salary")).collect()[0][0]
print(stddev_salary)

# Operations
df.withColumn("Full_Name", concat(col('first_name'), lit(" "), col('last_name'))).show(10, False)

# Grouping and Aggregation
df.groupBy("department").agg(round(avg('salary'),2).alias('avg_salary'), sum('salary').alias('sum_salary'), max('salary').alias('max_salary')).show(10,False)

# Joining
df2 = spark.read.option('header','true').csv('regions.csv',inferSchema=True)
df.join(df2, df.region_id == df2.region_id, "left").drop(df2.region_id).show(10, False)

# Pivot Table
df.groupBy("department").pivot("region_id").sum("salary").orderBy(col('1').desc()).show(10, False)

# Sorting
df.sort(col('salary').asc()).show(10, False)

# UDF (User Defined Functions)
def nano_seconds(name, frmt):
    if name and frmt == 'yyyy-MM-dd HH:mm:ss.SSSSSS':
        return datetime.datetime.strptime(name, '%Y-%m-%d %H:%M:%S.%f')
frmt = 'yyyy-MM-dd HH:mm:ss.SSSSSS'
nano_seconds_udf = udf(nano_seconds, TimestampType())
df.withColumn("Load_time", nano_seconds_udf(col('Load_time'), lit(frmt))).show(10, False)

# Complex operations
print("\nExploding an array column:")
df_with_array = df.withColumn("full_name", array("first_name", "last_name"))
df_with_array.select(col('*'), explode(col('full_name'))).show(10, False)
df_with_array.printSchema()

print("\nExtracting year and month from date:")
df.withColumn("year", year(col('hire_date'))).withColumn("month", month(col('hire_date'))).show()
print("Is DataFrame cached?:", df.is_cached)

# Working with RDDs
rdd = df.rdd.map(lambda row: (row.first_name, row.salary))
print(rdd.collect())

# Word count program
rdd = spark.sparkContext.textFile("word_count.txt")
print(len(set(rdd.flatMap(lambda line: line.split()).collect())))
word_counts = rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).collect()
for word, count in word_counts:
    print(f"{word}: {count}")

# second highest salary for each dept uing windowfunction
window_con  = Window.partitionBy("department").orderBy(col("salary").desc())
df1 = df.withColumn("dr",dense_rank().over(window_con))
df1.filter(col('dr') ==2).show()

# Word count in dataframe
df1 = spark.read.text("word_count.txt").withColumn("word", explode(split(col("value"), " "))).groupBy("word").count()
df1.select(sum('count')).show()

# transform() fucntion
def increase_salary(df, min_salary):
    return df.filter(col("salary") >= min_salary)
y = 100000
df.transform(lambda d: increase_salary(d, y)).show(10, False)

df.unpersist()
print("Is DataFrame cached?:", df.is_cached)