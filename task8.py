from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f


def task8(path_rating, path_to_save8, path_basics):
    # function for solving task8

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 8")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data, preparation data
    from_csv_ratings_df8 = spark_session.read.csv(path_rating, header=True, sep='\t', nullValue='null')
    from_csv_ratings_df8 = from_csv_ratings_df8.drop(f.col('numVotes'))
    from_csv_ratings_df8 = from_csv_ratings_df8.filter(f.col('averageRating') != r"\N")
    from_csv_ratings_df8 = from_csv_ratings_df8.withColumn('averageRating', f.col('averageRating').cast('double'))

    schema_tbasics = t.StructType([t.StructField("tconst", t.StringType(), True),
                                   t.StructField("titleType", t.StringType(), True),
                                   t.StructField("primaryTitle", t.StringType(), True),
                                   t.StructField("originalTitle", t.StringType(), True),
                                   t.StructField("isAdult", t.StringType(), True),
                                   t.StructField("startYear", t.DateType(), True),
                                   t.StructField("endYear", t.DateType(), True),
                                   t.StructField("runtimeMinutes", t.IntegerType(), True),
                                   t.StructField("genres", t.StringType(), True)])

    from_csv_basics_df8 = spark_session.read.csv(path_basics, header=True, sep='\t', nullValue='null',
                                                 schema=schema_tbasics, dateFormat='yyyy')
    from_csv_basics_df8 = from_csv_basics_df8.select(f.col('tconst'), f.col('originalTitle'), f.col('genres'))
    from_csv_basics_df8 = from_csv_basics_df8.filter(f.col('genres') != r"\N")

    # Main part
    # Creating list of genres
    # data_itr = from_csv_basics_df8.select(f.col('genres')).limit(10000).rdd.toLocalIterator()
    # # data_itr = from_csv_basics_df8.select(f.col('genres')).rdd.toLocalIterator()
    # all_genres_set=set()
    # for row in data_itr:
    #   str1=(row["genres"])
    #   list_str=str1.split(',')
    #   all_genres_set.update(list_str)

    # print(all_genres_set)
    # len(all_genres_set)
    all_genres_list = list({'Short', 'Documentary', 'Sci-Fi', 'Music', 'Thriller', \
                            'Adventure', 'Fantasy', 'Western', 'War', 'Family', 'Romance', \
                            'History', 'Drama', 'Comedy', 'News', 'Horror', 'Crime', 'Musical', \
                            'Animation', 'Sport', 'Mystery', 'Action', 'Biography'})

    # Join
    df8 = from_csv_ratings_df8.join(from_csv_basics_df8, 'tconst', how='left').drop(f.col('tconst'))

    # Creating output DF
    df8_n = df8.filter((f.col('genres').isin(all_genres_list[0]))) \
        .orderBy('averageRating', ascending=False).limit(10)
    df8_n = df8_n.withColumn('genres', f.lit(all_genres_list[0]))

    for i in range(1, len(all_genres_list)):
        df8_for_add = df8.filter((f.col('genres').isin(all_genres_list[i]))) \
            .orderBy('averageRating', ascending=False).limit(10)
        df8_for_add = df8_for_add.withColumn('genres', f.lit(all_genres_list[i]))
        df8_n = df8_n.unionByName(df8_for_add)

        # Writing data
    df8_n.write.csv(path_to_save8, header=True, mode="overwrite", sep='\t')
    return
