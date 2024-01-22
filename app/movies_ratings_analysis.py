from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, row_number, max, min, avg
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)

def create_spark_session(app_name="movies_ratings_analysis_app"):
    logging.info("Creating Spark session...")
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
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, file_path, schema):
    logging.info(f"Loading data from {file_path}...")
    try:
        return spark.read.format("csv").option("delimiter", "::").schema(schema).load(file_path)
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        raise

def calculate_movie_ratings(movies_df, ratings_df):
    logging.info("Calculating movie rating...")
    try:
        window_spec = Window.partitionBy("MovieID").orderBy("MovieID").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        return movies_df.join(ratings_df, "MovieID").withColumn("max_rating", max("Rating").over(window_spec)) \
                      .withColumn("min_rating", min("Rating").over(window_spec)) \
                      .withColumn("avg_rating", avg("Rating").over(window_spec))
    except Exception as e:
        logging.error(f"Error calculating movie ratings: {e}")
        raise

def get_top3_movies_by_user(ratings_df):
    logging.info("Getting top 3 movies for each user...")
    try:
        window_spec = Window.partitionBy("UserID").orderBy(col("Rating").desc())
        ranked_df = ratings_df.withColumn("rank", row_number().over(window_spec))
        return ranked_df.filter(col("rank") <= 3).select("UserID", "MovieID", "Rating", "Timestamp")
    except Exception as e:
        logging.error(f"Error getting top 3 movies by user: {e}")
        raise

def write_data(dataframe, output_path, mode="overwrite"):
    logging.info(f"Writing data to {output_path}...")
    try:
        dataframe.write.parquet(output_path, mode=mode)
    except Exception as e:
        logging.error(f"Error writing data to {output_path}: {e}")
        raise


def main():
    # Create a Spark session
    spark = create_spark_session()

    # input file path
    movies_file = "/content/drive/MyDrive/Colab Notebooks/data/NewDay/movies.dat"
    ratings_file = "/content/drive/MyDrive/Colab Notebooks/data/NewDay/ratings.dat"
    
    # Define schema for dataframes
    movies_schema = StructType([StructField('MovieID', IntegerType(), True), StructField('Title', StringType(), True), StructField('Genres', StringType(), True)])
    ratings_schema = StructType([StructField('UserID', IntegerType(), True), StructField('MovieID', IntegerType(), True), StructField('Rating', IntegerType(), True), StructField('Timestamp', StringType(), True)])
    
    #1. Read in movies.dat and ratings.dat to spark dataframes.
    # Load data
    movies_df = load_data(spark, movies_file, movies_schema)
    ratings_df = load_data(spark, ratings_file, ratings_schema)
    
    #2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and average rating for that movie from the ratings data.
    # Calculate movie ratings
    movies_with_rating_df = calculate_movie_ratings(movies_df, ratings_df)


    #3. Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.
    # Get top 3 movies by user
    top3_movies_df = get_top3_movies_by_user(ratings_df)

    #4. Write out the original and new dataframes in an efficient format of your choice.
    # Write data
    write_data(movies_df, "/content/drive/MyDrive/Colab Notebooks/data/NewDay/movies.parquet")
    write_data(ratings_df, "/content/drive/MyDrive/Colab Notebooks/data/NewDay/ratings.parquet")
    write_data(movies_with_rating_df, "/content/drive/MyDrive/Colab Notebooks/data/NewDay/movies_with_rating.parquet")
    write_data(top3_movies_df, "/content/drive/MyDrive/Colab Notebooks/data/NewDay/top3_movies.parquet")

    # Stop the Spark session
    logging.info("Stopping Spark session...")
    spark.stop()

if __name__ == "__main__":
    main()