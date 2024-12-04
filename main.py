from pyspark.sql import SparkSession

# start Spark session
spark = SparkSession.builder.appName("Simple Data Analysis").getOrCreate()

# read csv file
file_path = "data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# display the data
print("Data from csv file:")
df.show()

# Data Analysis: Avg wage based on City
avg_salary_by_city = df.groupBy("city").avg("salary")
print("Avg wage based on City:")
avg_salary_by_city.show()

# Filtering: Get employees that earn more than 100000
high_salary = df.filter(df.salary > 100000).orderBy(df.salary.desc())
print("Employees that earn more than 100000:")
high_salary.show()

# Terminate Spark session
spark.stop()
