from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
from datetime import date
import pyspark.sql.functions as f


spark_session = (SparkSession.builder
                            .master("local")
                            .appName("All tasks")
                            .config(conf=SparkConf())
                            .getOrCreate())

path_rating = r"imdb_data\title.ratings.tsv.gz"
path_to_save8 = "task_results\\Task8\\"

# path_basics = '/content/drive/MyDrive/Colab Notebooks/title.basics.tsv.gz'
from_csv_ratings_df  =  spark_session.read.csv(path_rating, header=True, sep='\t', nullValue='null')
# from_csv_ratings_df.show()
from_csv_ratings_df=from_csv_ratings_df.limit(20)

from_csv_ratings_df.write.csv(path_to_save8, header=True, mode="overwrite", sep='\t')