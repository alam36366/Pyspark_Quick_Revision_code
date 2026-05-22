from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType

spark = SparkSession.builder.master("local[*]") \
    .appName("PySparkExamples") \
    .config("spark.ui.port", "4050") \
    .enableHiveSupport() \
    .getOrCreate()

print("Sample data creation")
data = [(1, {"first_name": "Berrie", "Age": 29}, "bmanueau0@dion.ne.jp", "2006-04-20")]
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("Full_Name", StructType([  # Nested column
        StructField("first_name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("hire_date", StringType(), True)
])
df_man = spark.createDataFrame(data, schema)
# df_man.show(truncate=False)
# spark.sparkContext.setCheckpointDir("./Checkpoint_dir")
# df_man.checkpoint()

print("Reading from csv file")
df = spark.read.option('header', 'true') \
    .option('mode', 'DROPMALFORMED') \
    .option('delimiter', ',') \
    .csv('employees.csv', inferSchema=True)

df.createOrReplaceTempView("Employees_vw")
spark.sql("SELECT * FROM Employees_vw").show(10, False)
df = df.withColumn('Load_time', f.current_timestamp().cast(TimestampType()))
df.cache().show(10, False)
# df.explain(True)  # Gives you the Query plan, the physical plan

print("Basic operations")
df.select("first_name", "department", "salary").filter(f.col('salary') > 150000).show(10, False)
df.withColumn("Full_Name", f.concat(f.col('first_name'), f.lit(" "), f.col('last_name'))).show(10, False)

print("Handling missing data")
df = df.withColumn("email", f.when((f.col("email").isNull()) | (f.col("email") == 'NULL'), None).otherwise(f.col("email")))
print(df.na.drop().count())
df.fillna({"email": "Unknown", "salary": 0}).show(10, False)

print("Data Cleansing, Removing special characters from string columns in one go")
regex_pattern = "[^a-zA-Z0-9 @._]"  # retains alphanumeric characters, @._ and spaces only
dtypes = dict(df.dtypes)
df.select(*[
    f.regexp_replace(f.col(c), regex_pattern, "").alias(c) if dtypes[c] == 'string' else f.col(c) for c in df.columns
]).show(10, False)

print("Join with Broadcast-join along with grouping & aggregation")
df2 = spark.read.option('header','true').csv('regions.csv', inferSchema=True)
df.join(f.broadcast(df2), "region_id", "left") \
  .where((f.col("first_name").like("J%")) & (f.col("hire_date").between('2003-01-01', '2010-12-31'))) \
  .groupBy("region_id") \
  .agg(f.count("*").alias("emp_count"), f.sum("salary").alias("sum_salary")) \
  .filter(f.col("sum_salary") > 50000).show(10, False)

print("Broadcast variable") 
dept_dict = {'Maintenance': 101, 'Computers': 102, 'Security': 103, 'Sports': 104}
broadcast_department = spark.sparkContext.broadcast(dept_dict)
def map_department(dept_name):
    return broadcast_department.value.get(dept_name, None)
map_department_udf = f.udf(map_department, IntegerType())
df.withColumn("department", map_department_udf(df["department"])).show(10, False)
# blacklist_bc = spark.broadcast(set(load_from_db()))
# rdd.filter(lambda row: row.user_id not in blacklist_bc.value)

print("Complex operations")
df.groupBy("department").pivot("region_id").agg(f.sum("salary")).show(10, False)
df.withColumn("year", f.year(f.col('hire_date'))) .withColumn("month", f.month(f.col('hire_date'))).show(10, False)

print("Working with RDDs")
rdd = df.limit(10).rdd.filter(lambda x: x['salary'] > 120000)
print(rdd.collect())

print("Word count program with RDD")
rdd = spark.sparkContext.textFile("word_count.txt")
word_counts = rdd.flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y)\
    .collect()

for word, count in word_counts:
    print(f"{word}: {count}")

print("Word count with DataFrame")
spark.read.text("word_count.txt") \
    .withColumn("word", f.explode(f.split(f.col("value"), " "))) \
    .groupBy("word") \
    .count() \
    .show(10, False)

print("Window function: Second highest salary for each dept")
window_spec = Window.partitionBy("department").orderBy(f.col("salary").desc())
df.withColumn("dr", f.dense_rank().over(window_spec)).filter(f.col('dr') == 2).show(10, False)

window_spec = Window.partitionBy("department").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_spec = Window.partitionBy("department").orderBy("salary").rowsBetween(-2, 0)
df.withColumn("running_sal", f.sum("salary").over(window_spec)).show(10, False)

print("Transform function")
def good_salary(df, min_salary):
    return df.filter(f.col("salary") >= min_salary)
y = 100000
df.transform(lambda d: good_salary(d, y)).show(10, False)

"""print("Writing to PostgreSQL DB using JDBC")
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
"""

print("Using selectExpr and window via selectExpr")
df.selectExpr("Department", "case when salary > 100000 then salary end as salary").filter(f.col('salary').isNotNull()).show(10, False)
df.selectExpr("Department", "salary", "dense_rank() over(partition by department order by salary desc) as rn").filter(f.col('rn') == 1).show(10, False)

df.dropDuplicates(["employee_id", "email"]).show(10, False)
df.groupBy("department").agg(f.collect_list("salary")).show(10, False)

print("Repartition, coalesce, unpersist")
print("Is DataFrame cached?:", df.is_cached)

data = [
  ("Sahil", 12345, 1), ("Sahil", 99345, 2), ("Hitesh", 123455, 1),
  ("Hitesh", 993455, 2), ("Hitesh", 993455, 3), ("mukesh", 123455, 1), ("mukesh", 993455, 2)
]
columns = ["name", "phone", "rank"]
df = spark.createDataFrame(data, columns)
df = df.groupBy("name").pivot("rank").max("phone")
df.select([f.col(c).alias(f'phone_{c}') if c != "name" else f.col(c) for c in df.columns]).show()

import time
# time.sleep(1000)
"""spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4G \
  --executor-cores 2 \
  --num-executors 5 \
  --driver-memory 2G \
  --conf spark.executor.memoryOverhead=1024 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=1G \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.shuffle.partitions=100 \
  --conf spark.yarn.executor.memoryOverhead=1024 \
  path/to/your_script.py
"""