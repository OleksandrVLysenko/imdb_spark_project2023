from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f

def task1(path1, path_to_save1):
    # function for solving task1
    # path1 - input path
    # path_to_save1 - output path

    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 1")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    from_csv_title_akas_df1 = spark_session.read.csv(path1, header=True, sep='\t', nullValue='null')

    # Main part
    main_title_akas_df1 = from_csv_title_akas_df1.select(f.col('titleId'), f.col('title'), f.col('region'),
                                                         f.col('language'))
    uk_title_akas_df1 = main_title_akas_df1.filter((f.col('region') == 'UA') | (f.col('language') == 'uk'))
    uk_title_akas_dropDuplicates_df1 = uk_title_akas_df1.dropDuplicates(['titleId'])
    title_uk_df1_dropDuplicates = uk_title_akas_dropDuplicates_df1.select(f.col('title'))
    # title_uk_df1_dropDuplicates.filter(f.col('title').isin(r"\N",None)).show()

    # Writing data
    title_uk_df1_dropDuplicates.write.csv(path_to_save1, header=True, mode="overwrite", sep='\t')
    return

def task2(path2, path_to_save2):
    # function for solving task2
    # path2 - input path
    # path_to_save2 - output path

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 2")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    schema_df2 = t.StructType([t.StructField('nconst', t.StringType(), True),
                               t.StructField('primaryName', t.StringType(), True),
                               t.StructField('birthYear', t.DateType(), True),
                               t.StructField('deathYear', t.DateType(), True),
                               t.StructField('primaryProfession', t.StringType(), True),
                               t.StructField('knownForTitles', t.StringType(), True)])

    from_csv_name_basics_df2 = spark_session.read.csv(path2, header=True, sep='\t', nullValue='null',
                                                      schema=schema_df2, dateFormat='yyyy')

    # Main part
    date_1900 = date(1900, 1, 1)
    date_1800 = date(1800, 1, 1)
    names_19Centure_df2 = from_csv_name_basics_df2.filter(
        (f.col('birthYear') < date_1900) & (f.col('birthYear') > date_1800))
    names_19Centure_column_df2 = names_19Centure_df2.select(f.col('primaryName'))
    # names_19Centure_column_df2.show()
    names_19Centure_column_dropDuplicates_df2 = names_19Centure_column_df2.dropDuplicates()

    # Writing data
    names_19Centure_column_dropDuplicates_df2.write.csv(path_to_save2, header=True, mode="overwrite", sep='\t')
    return


def task3(path3, path_to_save3):
    # function for solving task3
    # path3 - input path
    # path_to_save3 - output path

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task 3")
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



