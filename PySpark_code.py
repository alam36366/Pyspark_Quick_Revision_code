from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, ArrayType, TimestampType

spark = SparkSession.builder.appName("PySparkExamples").getOrCreate()
# spark.sparkContext.setCheckpointDir("./Checkpoint_dir")

# Sample data creation
data = [(1, {"first_name": "Berrie","last_name": "Manueau"}, "bmanueau0@dion.ne.jp", "2006-04-20")]
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("Full_Name", StructType([  # Nested column
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("hire_date", StringType(), True)
])
# df = spark.createDataFrame(data, schema)

""" reading from csv file """
df = spark.read.option('header','true').csv('employees.csv', inferSchema=True)
print(df.count())
df = df.withColumn('Load_time', current_timestamp().cast(StringType()))
df.cache().show(10,False)
df.checkpoint()

""" Basic operations """
df.filter(col('salary') > 150000).select("first_name", "department", "salary").show(10, False)
df.withColumn("Full_Name", concat(col('first_name'), lit(" "), col('last_name'))).show(10, False)

"""Handling missing data"""
print(df.na.drop().count())
print("\nFilling NaN values with 0:")
df.withColumn("email", when((col("email").isNull()) | (col("email") == 'NULL'), 'Unknown').otherwise(col("email"))).show(10, False)

"""Removing spceial charecters"""
regex_pattern = "[^a-zA-Z0-9 ]"  # This pattern retains alphanumeric characters and spaces only
df.select([regexp_replace(col(c), regex_pattern, "").alias(c) if dict(df.dtypes)[c] == 'string' else col(c) for c in df.columns]).show(10, False)

"""Descriptive statistics"""
df.describe().show()
# Standard Deviation measures how much individual salaries deviate from the average salary.
stddev_salary = df.select(stddev("salary")).collect()[0][0]
print(stddev_salary)

"""Grouping and Aggregation"""
df.groupBy("department").agg(round(avg('salary'),2).alias('avg_salary'), sum('salary'), max('salary')).show(10,False)

"""Join with Broadcast join"""
df2 = spark.read.option('header','true').csv('regions.csv',inferSchema=True)
df.join(broadcast(df2), "region_id") \
.where((col("first_name").like("J%")) & (col("hire_date").between('2003-01-01', '2010-12-31'))) \
.groupBy("employee_id", "first_name") \
.agg(count("*").alias("cnt"), sum("salary").alias("sum_salary")) \
.where(col("sum_salary") > 50000) \
.select("employee_id", "first_name").show(10, False)

"""Pivot Table"""
df.groupBy("department").pivot("region_id").max("salary").orderBy(col('1').desc()).show(10, False)

"""Sorting"""
df.sort(col('salary').asc()).show(10, False)

"""UDF (User Defined Functions)"""
def nano_seconds(name, frmt):
    if name and frmt == 'yyyy-MM-dd HH:mm:ss.SSSSSS':
        return datetime.strptime(name, '%Y-%m-%d %H:%M:%S.%f')
frmt = 'yyyy-MM-dd HH:mm:ss.SSSSSS'
nano_seconds_udf = udf(nano_seconds, TimestampType())
df.withColumn("Load_time", nano_seconds_udf(col('Load_time'), lit(frmt))).show(10, False)

"""Complex operations"""
print("\nExploding an array column:")
df_with_array = df.withColumn("full_name", array("first_name", "last_name"))
df_with_array.select(col('*'), explode(col('full_name'))).show(10, False)
df_with_array.printSchema()

print("\nExtracting year and month from date:")
df.withColumn("year", year(col('hire_date'))).withColumn("month", month(col('hire_date'))).show(10, False)
print("Is DataFrame cached?:", df.is_cached)

"""Working with RDDs"""
rdd = df.rdd.map(lambda x: (x.first_name, x.salary))
print(rdd.collect()[2])
employee_rdd = spark.sparkContext.parallelize([
    {"name": "Alice", "age": 30, "salary": 60000},
    {"name": "Bob", "age": 25, "salary": 48000}])
filtered_rdd = employee_rdd.filter(lambda employee: employee['salary'] > 50000)
print(filtered_rdd.collect())

"""Word count program"""
rdd = spark.sparkContext.textFile("word_count.txt")
print(len(set(rdd.flatMap(lambda line: line.split()).collect())))
word_counts = rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).collect()
for word,count in word_counts:
    print(f"{word}: {count}")

"""Word count in dataframe"""
spark.read.text("word_count.txt").withColumn("word", explode(split(col("value"), " "))).groupBy("word").count().show(10, False)
# df1.select(sum('count')).show()

"""Second highest salary for each dept using windowfunction"""
window_con  = Window.partitionBy("department").orderBy(col("salary").desc())
df1 = df.withColumn("dr", dense_rank().over(window_con))
df1.filter(col('dr') == 2).show()

"""Transform() fucntion"""
def increase_salary(df, min_salary):
    return df.filter(col("salary") >= min_salary)
y = 100000
df.transform(lambda d: increase_salary(d, y)).show(10, False)

"""How to write to posgresql DB, using jdbc connection"""
df.write \
.format("jdbc") \
.option("url", "jdbc:postgresql://localhost:5432/course_data") \
.option("dbtable", "public.spark_table") \
.option("user", "postgres") \
.option("password", "postgres") \
.option("driver", "org.postgresql.Driver") \
.partitionBy("department") \
.mode("overwrite") \
.save()

""" Broadcast variable """
broadcast_department = spark.sparkContext.broadcast(department_map)
enriched_rdd = employee_rdd.map(lambda emp: (emp[0], emp[1], emp[2], broadcast_department.value.get(emp[1], "Unknown")))

df.repartition(2)
df.coalesce(1)
df.unpersist()
print("Is DataFrame cached?:", df.is_cached)