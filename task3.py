from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f

def task3(path3, path_to_save3):
    # function for solving task3
    # path3 - input path
    # path_to_save3 - output path

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 3")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    schema_df3 = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('titleType', t.StringType(), True),
                               t.StructField('primaryTitle', t.StringType(), True),
                               t.StructField('originalTitle', t.StringType(), True),
                               t.StructField('isAdult', t.StringType(), True),
                               t.StructField('startYear', t.DateType(), True),
                               t.StructField('endYear', t.DateType(), True),
                               t.StructField('runtimeMinutes', t.IntegerType(), True),
                               t.StructField('genres', t.StringType(), True)])

    from_csv_title_basics_df3 = spark_session.read.csv(path3, header=True, sep='\t', nullValue='null',
                                                       schema=schema_df3, dateFormat='yyyy')

    # Main part
    title_movie_more2hour_df3 = from_csv_title_basics_df3.filter(
        (f.col('titleType') == 'movie') & (f.col('runtimeMinutes') > 120))
    title_movie_more2hour_column_df3 = title_movie_more2hour_df3.select(f.col('primaryTitle'))
    title_movie_more2hour_column_dropDuplicates_df3 = title_movie_more2hour_column_df3.dropDuplicates()

    # Writing data
    title_movie_more2hour_column_dropDuplicates_df3.write.csv(path_to_save3, header=True, mode="overwrite", sep='\t')