import logging
from jobs.aggregates.functions import (
    sum_accidents_per_location_and_day,
    group_day_night_columns,
    group_temperature,
    group_humidity,
    group_precipitation,
    group_true_false_columns,
    group_wind_speed,
    group_weather_condition,
    sum_accidents_per_hour,
    group_visibility
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _extract_table(spark, table_name):
    """
    Extract data from a BigQuery table.

    Parameters:
    spark (SparkSession): The Spark session.
    table_name (str): The name of the table to extract data from.

    Returns:
    DataFrame: The extracted data as a Spark DataFrame.
    """
    logger.info(f"Extracting data from table: accidents.{table_name}")
    return (
        spark.read
            .format('com.google.cloud.spark.bigquery')
            .option('table', f'accidents.{table_name}')
            .load()
    )


def _transform_data(usa_accidents_df, function_name):
    """
    Transform data using the specified aggregation function.

    Parameters:
    usa_accidents_df (DataFrame): The DataFrame containing USA accidents data.
    function_name (function): The aggregation function to apply.

    Returns:
    DataFrame: The transformed DataFrame with the applied aggregation.
    """
    logger.info(f"Transforming data using function: {function_name.__name__}")
    return function_name(usa_accidents_df)


def _load_data(aggregation_table, table_name):
    """
    Load data into a BigQuery table.

    Parameters:
    aggregation_table (DataFrame): The DataFrame to load into BigQuery.
    table_name (str): The name of the target BigQuery table.
    """
    logger.info(f"Loading data into table: accidents.{table_name}")
    aggregation_table.write.format("com.google.cloud.spark.bigquery") \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", "example_bucket_name") \
        .save(f'accidents.{table_name}')


def _run_job(spark):
    """
    Run ETL job to create daily aggregates per location for various metrics.

    Parameters:
    spark (SparkSession): The Spark session.
    """
    logger.info("Starting ETL job")

    # Extract data
    usa_accidents_df = _extract_table(spark, 'curated_data')

    # List of aggregation functions and corresponding table names
    aggregation_tasks = [
        (sum_accidents_per_location_and_day, 'accidents_per_location_and_day'),
        (sum_accidents_per_hour, 'accidents_per_hour'),
        (group_precipitation, 'precipitation_ranges_per_location_and_day'),
        (group_weather_condition, 'accidents_per_weather_conditions'),
        (group_true_false_columns, 'true_false_columns_per_location_and_day'),
        (group_day_night_columns, 'day_night_columns_per_location_and_day'),
        (group_humidity, 'humidity_ranges_per_location_and_day'),
        (group_wind_speed, 'wind_speed_ranges_per_location_and_day'),
        (group_temperature, 'temperature_ranges_per_location_and_day'),
        (group_visibility, 'visibility_ranges_per_location_and_day')
    ]

    # Transform and load data for each aggregation task
    for function_name, table_name in aggregation_tasks:
        logger.info(f"Processing task for table: {table_name}")
        transformed_data = _transform_data(usa_accidents_df, function_name)
        _load_data(transformed_data, table_name)

    logger.info("ETL job completed")
