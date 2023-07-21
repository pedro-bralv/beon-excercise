from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/home/pedrobralv/projects/data-engineer-coding-interview/challenge1/spark/postgresql-driver/postgresql-42.6.0.jar") \
    .master("local") \
    .getOrCreate()
