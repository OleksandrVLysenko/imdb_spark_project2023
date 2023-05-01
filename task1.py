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
