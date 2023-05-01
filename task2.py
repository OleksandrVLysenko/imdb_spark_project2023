from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f

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