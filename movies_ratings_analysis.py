from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, row_number, max, min, avg
from pyspark.sql.window import Window


if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder \
            .appName("movies_ratings_analysis_app") \
            .getOrCreate()
    
    # If you are running spark on a cluster you can change and add below spark configurations accordingly
            # .config("spark.driver.memory","90g") \
            # .config("spark.driver.cores",8) \
            # .config("spark.driver.maxResultSize", "8g") \
            # .config("spark.executor.cores",8) \
            # .config("spark.executor.instances",2) \
            # .config("spark.executor.memory","30g") \
            # .config("spark.default.parallelism", 16) \
            # .config("spark.sql.shuffle.partitions", 16) \
            # .getOrCreate()

    movies_file = "/content/drive/MyDrive/Colab Notebooks/data/NewDay/movies.dat"
    movies_schema = StructType([StructField('MovieID', IntegerType(), True), StructField('Title', StringType(), True), StructField('Genres', StringType(), True)])
    ratings_file = "/content/drive/MyDrive/Colab Notebooks/data/NewDay/ratings.dat"
    ratings_schema = StructType([StructField('UserID', IntegerType(), True), StructField('MovieID', IntegerType(), True), StructField('Rating', IntegerType(), True), StructField('Timestamp', StringType(), True)])
    users_file = "/content/drive/MyDrive/Colab Notebooks/data/NewDay/users.dat"
    users_schema = StructType([StructField('UserID', IntegerType(), True), StructField('Gender', StringType(), True), StructField('Age', IntegerType(), True), StructField('Occupation', StringType(), True), StructField('Zip-code', StringType(), True)])

    #1. Read in movies.dat and ratings.dat to spark dataframes.
    movies_df = spark.read.format("csv").option("delimiter", "::").schema(movies_schema).load(movies_file)
    ratings_df = spark.read.format("csv").option("delimiter", "::").schema(ratings_schema).load(ratings_file)

    #2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and average rating for that movie from the ratings data.
    users_df = spark.read.format("csv").option("delimiter", "::").schema(users_schema).load(users_file)
    movies_join_rating_df = movies_df.join(ratings_df, "MovieID")
    window_spec = Window.partitionBy("MovieID").orderBy("MovieID").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    movies_with_rating_df = movies_join_rating_df.withColumn("max_rating", max("Rating").over(window_spec)) \
                    .withColumn("min_rating", min("Rating").over(window_spec)) \
                    .withColumn("avg_rating", avg("Rating").over(window_spec))

    #3. Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.
    window_spec = Window.partitionBy("UserID").orderBy(col("Rating").desc())
    ranked_df = ratings_df.withColumn("rank", row_number().over(window_spec))
    top3_movies_df = ranked_df.filter(col("rank") <= 3).select("UserID", "MovieID", "Rating", "Timestamp")

    #4. Write out the original and new dataframes in an efficient format of your choice.
    movies_df.write.parquet("/content/drive/MyDrive/Colab Notebooks/data/NewDay/users.parquet", mode="overwrite")
    ratings_df.write.parquet("/content/drive/MyDrive/Colab Notebooks/data/NewDay/ratings.parquet", mode="overwrite")
    users_df.write.parquet("/content/drive/MyDrive/Colab Notebooks/data/NewDay/users.parquet", mode="overwrite")
    movies_with_rating_df.write.parquet("/content/drive/MyDrive/Colab Notebooks/data/NewDay/movies_with_rating.parquet", mode="overwrite")
    top3_movies_df.write.parquet("/content/drive/MyDrive/Colab Notebooks/data/NewDay/top3_movies.parquet", mode="overwrite")

    # Stop the Spark session
    spark.stop()