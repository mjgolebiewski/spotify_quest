from pyspark.sql import SparkSession
from pyspark import SparkConf
from IPython.display import display
from pyspark.sql.functions import *
from pyspark.sql import DataFrame, Window

from spotify_secret import spark_config, hdfs_path

conf = SparkConf().setAll([(key, value) for key, value in spark_config.items()])

spark = SparkSession \
            .builder \
            .master('yarn') \
            .config(conf=conf) \
            .getOrCreate()

spotify_path = f'{hdfs_path}/data_by_year.csv'
spotify = spark.read.format('csv').options(header='true', inferSchema='true').load(spotify_path)

spotify = spotify.select('year', 'danceability', 'energy')

w = Window.orderBy(spotify.year.desc())

spotify = spotify \
                .withColumn('danceability_delta', spotify.danceability - lead('danceability').over(w)) \
                .withColumn('energy_delta', spotify.energy - lead('energy').over(w))

spotify = spotify \
                .withColumn('danceability_delta', spotify.danceability_delta.cast(DecimalType(20, 16))) \
                .withColumn('energy_delta', spotify.energy_delta.cast(DecimalType(20, 16)))

spotify.coalesce(1).write.csv(f'{hdfs_path}/spotify.csv', header = True)
