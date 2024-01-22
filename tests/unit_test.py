import pytest
from unittest.mock import patch, Mock
from app.movies_ratings_analysis import load_data, calculate_movie_ratings, get_top3_movies_by_user, write_data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

logging.basicConfig(level=logging.INFO)

@pytest.fixture
def spark_session():
    logging.info("Creating Spark session...")
    from pyspark.sql import SparkSession
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_load_data(spark_session):
    logging.info("executing test_load_data")
    schema = StructType([StructField('MovieID', IntegerType(), True), StructField('Title', StringType(), True), StructField('Genres', StringType(), True)])
    
    # Mocking the actual spark.read method
    with patch("pyspark.sql.DataFrameReader.load") as mock_load:
        load_data(spark_session, "test_path", schema)
        # Assert that the correct arguments were passed to spark.read
        mock_load.assert_called_with("test_path")

def test_calculate_movie_ratings(spark_session):
    logging.info("executing test_calculate_movie_ratings")
    # Sample data for testing
    movies_data = [(1, 'Movie1', 'Action'), (2, 'Movie2', 'Drama')]
    ratings_data = [(1, 1, 5, '2022-01-01'), (2, 2, 4, '2022-01-02')]

    movies_schema = StructType([StructField('MovieID', IntegerType(), True),
                                StructField('Title', StringType(), True),
                                StructField('Genres', StringType(), True)])

    ratings_schema = StructType([StructField('UserID', IntegerType(), True),
                                 StructField('MovieID', IntegerType(), True),
                                 StructField('Rating', IntegerType(), True),
                                 StructField('Timestamp', StringType(), True)])

    movies_df = spark_session.createDataFrame(movies_data, schema=movies_schema)
    ratings_df = spark_session.createDataFrame(ratings_data, schema=ratings_schema)

    # Ensure that the function executes without errors
    try:
        result_df = calculate_movie_ratings(movies_df, ratings_df)
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")

    # Validate the structure of the result DataFrame
    expected_columns = ['MovieID', 'Title', 'Genres', 'UserID', 'Rating', 'Timestamp', 'max_rating', 'min_rating', 'avg_rating']
    assert result_df.columns == expected_columns

    # Validate the content of the result DataFrame
    expected_data = [(1, 'Movie1', 'Action', 1, 5, '2022-01-01', 5, 5, 5.0),
                     (2, 'Movie2', 'Drama', 2, 4, '2022-01-02', 4, 4, 4.0)]
    assert result_df.collect() == expected_data

def test_get_top3_movies_by_user(spark_session):
    logging.info("executing test_get_top3_movies_by_user")
    # Sample data for testing
    data = [(1, 1, 5, '2022-01-01'), (1, 2, 4, '2022-01-02'),
            (2, 1, 3, '2022-01-03'), (2, 3, 2, '2022-01-04')]

    schema = StructType([StructField('UserID', IntegerType(), True),
                        StructField('MovieID', IntegerType(), True),
                        StructField('Rating', IntegerType(), True),
                        StructField('Timestamp', StringType(), True)])

    ratings_df = spark_session.createDataFrame(data, schema=schema)

    # Ensure that the function executes without errors
    try:
        result_df = get_top3_movies_by_user(ratings_df)
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")

    # Validate the structure of the result DataFrame
    expected_columns = ['UserID', 'MovieID', 'Rating', 'Timestamp']
    assert result_df.columns == expected_columns

    # Validate the content of the result DataFrame
    expected_data = [(1, 1, 5, '2022-01-01'), (1, 2, 4, '2022-01-02'), (2, 1, 3, '2022-01-03')]
    assert result_df.collect() == expected_data

def test_write_data(spark_session):
    logging.info("executing test_write_data")
    # Mocking the actual write operation
    with patch.object(spark_session, "createDataFrame") as mock_create_df:
        with patch.object(mock_create_df.return_value, "write") as mock_write:
            # Create a mock DataFrame
            dataframe = spark_session.createDataFrame([(1, 'Movie1', 'Action'), (2, 'Movie2', 'Drama')],
                                                       ['MovieID', 'Title', 'Genres'])

            # Call the write_data function with the mock DataFrame
            write_data(dataframe, "test_path")

            mock_write.parquet.assert_called_with("test_path", mode="overwrite")