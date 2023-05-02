from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f


def task5(path5, path_to_save5, path5add_region):
    # function for solving task5
    def join_function(df5):
        df5 = df5.select(f.col('tconst'), f.col('originalTitle'))
        df5 = df5.join(from_csv_region_df, df5.tconst == from_csv_region_df.titleId, how='left').drop(f.col('titleId'))
        df5 = df5.filter(f.col('region') != r"\N")
        df5 = df5.dropDuplicates()
        df5 = df5.groupBy('region').count().orderBy('count', ascending=False).limit(100)
        return df5

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 5")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    from_csv_title_basics_df5 = spark_session.read.csv(path5, header=True, sep='\t', nullValue='null')
    from_csv_region_df = spark_session.read.csv(path5add_region, header=True, sep='\t', nullValue='null')

    # Data preparation
    from_csv_title_basics_df5 = from_csv_title_basics_df5.select('tconst', 'titleType', 'originalTitle', 'isAdult')
    from_csv_region_df = from_csv_region_df.select(f.col('titleId'), f.col('region'))
    from_csv_region_df = from_csv_region_df.filter(f.col('region') != r"\N")
    title_basics_isAdult_df5 = from_csv_title_basics_df5.filter(f.col('isAdult') == '1')

    # Main part
    # movie_df5 = title_basics_isAdult_df5.filter(f.col('titleType').isin('movie'))
    # movie_join_groupBy_region_df5 = join_function(movie_df5)
    movie_all_join_groupBy_region_df5 = join_function(title_basics_isAdult_df5)

    # Writing data
    movie_all_join_groupBy_region_df5.write.csv(path_to_save5, header=True, mode="overwrite", sep='\t')
    return

