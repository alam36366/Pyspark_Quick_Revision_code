from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import datetime
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType

spark = SparkSession.builder.master("local[*]").appName("PySparkExamples").getOrCreate()

# Sample data creation
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
# df = spark.createDataFrame(data, schema)

""" reading from csv file """
df = spark.read.option('header','true').csv('employees.csv', inferSchema=True)
# print(df.count())
# df = df.withColumn('Load_time', current_timestamp().cast(StringType()))
# df.cache().show(10,False)
# """ spark.sparkContext.setCheckpointDir("./Checkpoint_dir") 
#     df.checkpoint() """

# """ Basic operations """
# df.filter(col('salary') > 150000).select("first_name", "department", "salary").show(10, False)
# df.withColumn("Full_Name", concat(col('first_name'), lit(" "), col('last_name'))).show(10, False)

# """Handling missing data"""
# print(df.na.drop().count())
# df.withColumn("email", when((col("email").isNull()) | (col("email") == 'NULL'), 'Unknown').otherwise(col("email"))).show(10, False) # if otherwise not applied, unmatched values will be null

# """Removing spceial charecters"""
# regex_pattern = "[^a-zA-Z0-9 ]"  # This pattern retains alphanumeric characters and spaces only
# df.select(*[regexp_replace(col(c), regex_pattern, "").alias(c) if dict(df.dtypes)[c] == 'string' else col(c) for c in df.columns]).show(10, False)

"""Join with Broadcast-join along with grouping & aggrigation"""
df2 = spark.read.option('header','true').csv('regions.csv',inferSchema=True)
df.join(broadcast(df2), "region_id") \
.where((col("first_name").like("J%")) & (col("hire_date").between('2003-01-01', '2010-12-31'))) \
.groupBy("employee_id", "first_name") \
.agg(count("*").alias("cnt"), sum("salary").alias("sum_salary")) \
.where(col("sum_salary") > 50000) \
.select("employee_id", "first_name").show(10, False)

# """Pivot Table"""
# df.groupBy("department").pivot("region_id").max("salary").orderBy(col('1').desc()).show(10, False)
# df.sort(col('salary').asc()).show(10, False)

# """Broadcast variable """
# distinct_departments = df.select('department').distinct().rdd.flatMap(lambda x: x).collect()
# department_dict = {dept: 101 + idx for idx, dept in enumerate(distinct_departments)}
# broadcast_department = spark.sparkContext.broadcast(department_dict)
# department_dict = broadcast_department.value
# mapping_expr = create_map([lit(x) for x in chain(*department_dict.items())])
# df.withColumn("department", mapping_expr[df["department"]]).show(10, False)

# """Second way of broadcast variable using UDF"""
# def map_department(dept_name):
#     return broadcast_department.value.get(dept_name, None)
# map_department_udf = udf(map_department, IntegerType())
# df.withColumn("department", map_department_udf(df["department"])).show(10, False)

# """Complex operations"""
# print("\nExploding an array column:")
# df_with_array = df.withColumn("full_name", array("first_name", "last_name"))
# df_with_array.select(col('*'), explode(col('full_name'))).show(10, False)
# df_with_array.printSchema()
# df.withColumn("year", year(col('hire_date'))).withColumn("month", month(col('hire_date'))).show(10, False)

# """Working with RDDs"""
# rdd = df.rdd.map(lambda x: (x.first_name, x.salary))
# print(rdd.collect()[2])
# employee_rdd = spark.sparkContext.parallelize([{"name": "Alice", "age": 30, "salary": 60000},
#     {"name": "Bob", "age": 25, "salary": 48000}])
# filtered_rdd = employee_rdd.filter(lambda employee: employee['salary'] > 50000)
# print(filtered_rdd.collect())

# """Word count program with rdd"""
# rdd = spark.sparkContext.textFile("word_count.txt")
# print(len(set(rdd.flatMap(lambda line: line.split()).collect())))
# word_counts = rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).collect()
# for word,count in word_counts:
#     print(f"{word}: {count}")

# """Word count with dataframe"""
# spark.read.text("word_count.txt").withColumn("word", explode(split(col("value"), " "))).groupBy("word").count().show(10, False)
# # df1.select(sum('count')).show()

# """Window function, Second highest salary for each dept using windowfunction"""
# window_con  = Window.partitionBy("department").orderBy(col("salary").desc())
# df1 = df.withColumn("dr", dense_rank().over(window_con))
# df1.filter(col('dr') == 2).show()
# window_spec = Window.partitionBy("department").orderBy("salary")
# df.withColumn("prev_value", lag("salary", 1).over(window_spec)) \
#   .withColumn("next_value", lead("salary", 1).over(window_spec)).show()

# """Transform() fucntion"""
# def increase_salary(df, min_salary):
#     return df.filter(col("salary") >= min_salary)
# y = 100000
# df.transform(lambda d: increase_salary(d, y)).show(10, False)

# """How to write to posgresql DB, using jdbc connection"""
# df.write \
# .format("jdbc") \
# .option("url", "jdbc:postgresql://localhost:5432/course_data") \
# .option("dbtable", "public.spark_table") \
# .option("user", "postgres") \
# .option("password", "postgres") \
# .option("driver", "org.postgresql.Driver") \
# .partitionBy("department") \
# .mode("overwrite") \
# .save()

# df.selectExpr("Department", "case when salary > 100000 then salary end as salary").filter(col('salary').isNotNull()).show(10, False)
# df.dropDuplicates(["employee_id", "email"]).show(10, False)
# df.groupBy("department").agg(collect_list("salary")).show(10, False)
# """df.repartition(2),  df.coalesce(1), df.unpersist()"""
# print("Is DataFrame cached?:", df.is_cached)