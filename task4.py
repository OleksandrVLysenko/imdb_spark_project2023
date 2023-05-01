from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f

def task4(path4, path_to_save4, path4add_name, path4add_title):
    # function for solving task4
    # path4 - input path
    # path_to_save4 - output path
    # path4add_name - path the name table
    # path4add_title - path the title table

    # Creation of SparkSession (the entrypoint of the PySpark application)
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("Task 4")
                     .config(conf=SparkConf())
                     .getOrCreate())

    # Reading data
    from_csv_principals_df4 = spark_session.read.csv(path4, header=True, sep='\t', nullValue='null')
    from_csv_name_df = spark_session.read.csv(path4add_name, header=True, sep='\t', nullValue='null')
    from_csv_title_df = spark_session.read.csv(path4add_title, header=True, sep='\t', nullValue='null')

    # Main part
    actor_df4 = from_csv_principals_df4.filter(f.col('category') == 'actor')  # 12326629
    actor_short_df4 = actor_df4.drop(f.col('ordering'), f.col('job'), f.col('category'))
    actor_short_df4 = actor_short_df4.withColumn('characters', f.when(f.col('characters').isin(r"\N", None), None)
                                                 .otherwise(f.col('characters')))
    # actor_short_df4.show(truncate=False)
    name_short_df = from_csv_name_df.select(f.col('nconst'), f.col('primaryName'))
    # name_short_df.filter(f.col('primaryName')==r"\N").show()
    title_short_df = from_csv_title_df.select(f.col('tconst'), f.col('primaryTitle'))
    expanded_actor1_df = actor_short_df4.join(title_short_df, on='tconst', how='left')
    expanded_actor2_df = expanded_actor1_df.join(name_short_df, on='nconst', how='left').drop(f.col('nconst'),
                                                                                              f.col('tconst'))
    expanded_actor2_dropDuplicates_df = expanded_actor2_df.dropDuplicates()

    # Writing data
    expanded_actor2_dropDuplicates_df.write.csv(path_to_save4, header=True, mode="overwrite", sep='\t')
    return
