import argparse
from load_dw.sparksession import spark

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest_host', dest='dest_host', type=str, default="localhost")
    parser.add_argument('--dest_port', dest='dest_port', type=int, default=5432)
    parser.add_argument('--dest_username', dest='dest_username', type=str, default="postgres")
    parser.add_argument('--dest_password', dest='dest_password', type=str, default="Password1234**")
    parser.add_argument('--dest_database', dest='dest_database', type=str, default="dw_flights")
    parser.add_argument('--dest_table', dest='dest_table', type=str)
    parser.add_argument('--source', dest='source', type=str)
    known_args, pipeline_args = parser.parse_known_args(argv)
    

    url = f"jdbc:postgresql://{known_args.dest_host}:{known_args.dest_port}/{known_args.dest_database}"
    properties = {
        "user": known_args.dest_username,
        "password": known_args.dest_password,
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.options(
        inferSchema=True,
        header=True, 
        delimiter=",").csv(known_args.source)

    df.printSchema()

    if known_args.dest_table == "flights" :
        df = df.drop("_c0")
        df = df.withColumnRenamed("dep_time", "actual_dep_time") \
            .withColumnRenamed("arr_time", "actual_arr_time")

    if known_args.dest_table == "planes":
        df = df.withColumn("year", df.year.cast("int"))
        df = df.withColumn("speed", df.speed.cast("double"))

    if known_args.dest_table == "airports":
        df = df.withColumnRenamed("lat", "latitude")
        df = df.withColumnRenamed("lon", "longitude")
        df = df.withColumnRenamed("alt", "altitude")
        df = df.withColumnRenamed("tz", "timezone")
        df = df.withColumnRenamed("tzone", "timezone_name")

        df = df.withColumn("altitude", df.altitude.cast("double"))
    


    df.write.jdbc(url, known_args.dest_table, mode="append", properties=properties)
