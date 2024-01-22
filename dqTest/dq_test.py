from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
import pytest
import logging

logging.basicConfig(level=logging.INFO)

@pytest.fixture
def spark_session():
    logging.info("Creating Spark session...")
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_movies_data_quality(spark_session):
    logging.info("executing test_movies_data_quality")
    # Define paths and schemas
    movies_file = "./data/movies.dat"
    movies_schema = StructType([
        StructField('MovieID', IntegerType(), True),
        StructField('Title', StringType(), True),
        StructField('Genres', StringType(), True)
    ])

    # Read movies data
    movies_df = spark_session.read.format("csv").option("delimiter", "::").schema(movies_schema).load(movies_file)

    # Check for null values
    assert not any([(c, movies_df.where(col(c).isNull()).count()) for c in movies_df.columns]), "Null values found in movies data"

    # Check schema validation
    assert movies_df.schema == movies_schema, "Movies data schema does not match expected schema"

def test_ratings_data_quality(spark_session):
    logging.info("executing test_ratings_data_quality")
    # Define paths and schemas
    ratings_file = "./data/ratings.dat"
    ratings_schema = StructType([
        StructField('UserID', IntegerType(), True),
        StructField('MovieID', IntegerType(), True),
        StructField('Rating', IntegerType(), True),
        StructField('Timestamp', StringType(), True)
    ])

    # Read ratings data
    ratings_df = spark_session.read.format("csv").option("delimiter", "::").schema(ratings_schema).load(ratings_file)

    # Check for null values
    assert not any([(c, ratings_df.where(col(c).isNull()).count()) for c in ratings_df.columns]), "Null values found in ratings data"

    # Check schema validation
    assert ratings_df.schema == ratings_schema, "Ratings data schema does not match expected schema"
