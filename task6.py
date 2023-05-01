from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f


def task6(path6, path_to_save6, path6add_basics):
    # function for solving task6

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 6")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    from_csv_episode_df6 = spark_session.read.csv(path6, header=True, sep='\t', nullValue='null')
    from_csv_basics_df6 = spark_session.read.csv(path6add_basics, header=True, sep='\t', nullValue='null')

    # Main part
    from_csv_basics_df6 = from_csv_basics_df6.select(f.col('tconst'), f.col('originalTitle'))
    df6 = from_csv_episode_df6.groupBy('parentTconst').count().orderBy('count', ascending=False).limit(50)
    df6 = df6.join(from_csv_basics_df6, df6.parentTconst == from_csv_basics_df6.tconst, how='left')
    df6 = df6.select(f.col('originalTitle'), f.col('count')).orderBy('count', ascending=False).limit(50)

    # Writing data
    df6.write.csv(path_to_save6, header=True, mode="overwrite", sep='\t')
    return

