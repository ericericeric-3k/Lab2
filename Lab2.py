from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProcessStudentData") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

dataset = spark.read.csv("dataset/ultimate_student_productivity_dataset_5000.csv", header=True, inferSchema=True)


