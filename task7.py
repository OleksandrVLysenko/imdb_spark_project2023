from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f


def task7(path7, path_to_save7, path7add_basics):
    # function for solving task7

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task 7")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data, preparation data
    from_csv_ratings_df7 = spark_session.read.csv(path7, header=True, sep='\t', nullValue='null')
    from_csv_ratings_df7 = from_csv_ratings_df7.drop(f.col('numVotes'))
    from_csv_ratings_df7 = from_csv_ratings_df7.filter(f.col('averageRating') != r"\N")
    schema_tbasics = t.StructType([t.StructField("tconst", t.StringType(), True),
                                   t.StructField("titleType", t.StringType(), True),
                                   t.StructField("primaryTitle", t.StringType(), True),
                                   t.StructField("originalTitle", t.StringType(), True),
                                   t.StructField("isAdult", t.StringType(), True),
                                   t.StructField("startYear", t.DateType(), True),
                                   t.StructField("endYear", t.DateType(), True),
                                   t.StructField("runtimeMinutes", t.IntegerType(), True),
                                   t.StructField("genres", t.StringType(), True)])

    from_csv_basics_df7 = spark_session.read.csv(path7add_basics, header=True, sep='\t', nullValue='null',
                                                 schema=schema_tbasics, dateFormat='yyyy')
    from_csv_basics_df7 = from_csv_basics_df7.select(f.col('tconst'), f.col('originalTitle'), f.col('startYear'))
    df7 = from_csv_ratings_df7.join(from_csv_basics_df7, 'tconst', how='left').drop(f.col('tconst'))
    df7 = df7.withColumn('averageRating', f.col('averageRating').cast('double'))
    df7 = df7.orderBy('startYear', ascending=False)

    # Main part
    st_date = 2020;
    end_date = 2010
    df7_n = df7.filter((f.col('startYear') <= date(st_date, 1, 1)) & (f.col('startYear') > date(end_date, 1, 1))) \
        .orderBy('averageRating', ascending=False).limit(10)

    list_years = [2020, 2010, 2000, 1990, 1980, 1970, 1960, 1950, 1940, 1930, 1920, 1910, 1900, 1890, 1880, 1870]
    for i in range(1, len(list_years) - 1):
        st_date = list_years[i];
        end_date = list_years[i + 1]
        df7_for_add = df7.filter(
            (f.col('startYear') <= date(st_date, 1, 1)) & (f.col('startYear') > date(end_date, 1, 1))) \
            .orderBy('averageRating', ascending=False).limit(10)
        df7_n = df7_n.unionByName(df7_for_add)

        # Writing data
    df7_n.write.csv(path_to_save7, header=True, mode="overwrite", sep='\t')
    return


