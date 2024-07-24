import pyspark
from pyspark.sql.functions import col, to_date, concat_ws, when, hour
import numpy as np


def sum_accidents_per_hour(usa_accidents_df):
    """
    Creating aggregate for number of accidents per hour.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Extracted table of accidents from CSV converted to Pyspark Dataframe.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Table of number of accidents per day per hour.
    """
    # Extract the date part from 'Start_Time'
    accidents_per_hour = usa_accidents_df.withColumn('Date', to_date(col('Start_Time')))

    # Extract the hour part from 'Start_Time'
    accidents_per_hour = accidents_per_hour.withColumn('Hour', hour(col('Start_Time')))

    # Group by date and hour, then count the number of accidents
    accidents_per_hour = accidents_per_hour.groupBy('Date', 'Hour', 'County', 'State', 'Country').count()

    # Select only the necessary columns
    accidents_per_hour = accidents_per_hour.select('Date', 'Hour', 'County', 'State', 'Country', 'count')

    return accidents_per_hour


def sum_accidents_per_location_and_day(usa_accidents_df):
    """
    Creating aggregate for number of accidents per day per location.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Extracted table of accidents from CSV converted to Pyspark Dataframe.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Table of number of accidents per day per location.
    """
    # Extract the date part from 'Start_Time'
    num_of_accidents = usa_accidents_df.withColumn('Date', to_date(col('Start_Time')))

    # Group by date and location, then count the number of accidents
    num_of_accidents = num_of_accidents.groupBy('Date', 'County', 'State', 'Country').count()

    # Combine columns into one for the address
    num_of_accidents = num_of_accidents.withColumn(
        'Address',
        concat_ws(', ', col('City'), col('County'), col('State'), col('Country'))
    )

    # Select only the necessary columns
    num_of_accidents = num_of_accidents.select('Date', 'Address', 'City', 'State', 'count')

    return num_of_accidents


def group_visibility(
        usa_accidents_df, visibility_column="Visibility_mi", labeled_visibility_column="visibility_label"
):
    """
    Group visibility data into labeled ranges and count occurrences.

    This function takes a DataFrame with accident data, converts the visibility values
    from miles to kilometers, labels the precipitation data into specified ranges,
    and then groups the data by these labels to count the number of occurrences in each range.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data.
    visibility_column : str
        The name of the column in `usa_accidents_df` that contains the precipitation values in miles.
    labeled_visibility_column : str
        The name of the new column to be created in the DataFrame with labeled precipitation ranges.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with labeled precipitation ranges and the count of occurrences in each range.
    """
    # Transform miles to kilometers
    grouped_visibility_df = usa_accidents_df.withColumn(
        visibility_column, col(visibility_column) * 1.60934)

    grouped_visibility_df = grouped_visibility_df.withColumn(
        labeled_visibility_column,
        when((col(visibility_column) >= 0) & (col(visibility_column) <= 1), '0-1 km')
        .when((col(visibility_column) > 1) & (col(visibility_column) <= 2), '1-2 km')
        .when((col(visibility_column) > 2) & (col(visibility_column) <= 3), '2-3 km')
        .when((col(visibility_column) > 3) & (col(visibility_column) <= 4), '3-4 km')
        .when((col(visibility_column) > 4) & (col(visibility_column) <= 5), '4-5 km')
        .when((col(visibility_column) > 5) & (col(visibility_column) <= 6), '5-6 km')
        .when((col(visibility_column) > 6) & (col(visibility_column) <= 7), '6-7 km')
        .when((col(visibility_column) > 7) & (col(visibility_column) <= 8), '7-8 km')
        .when((col(visibility_column) > 8) & (col(visibility_column) <= 9), '8-9 km')
        .when((col(visibility_column) > 9) & (col(visibility_column) <= 10), '9-10 km')
        .when((col(visibility_column).isNull()), np.nan)
        .otherwise('+10 km')  # Just in case there are values outside the expected range
    )

    # Extract the date part from 'Start_Time'
    grouped_visibility_df = grouped_visibility_df.withColumn('Date', to_date(col('Start_Time')))

    grouped_visibility_df = grouped_visibility_df.groupBy(labeled_visibility_column, 'Date', 'County', 'State',
                                                          'Country') \
        .count()

    grouped_visibility_df = grouped_visibility_df.select(labeled_visibility_column, 'count')

    return grouped_visibility_df


def group_weather_condition(usa_accidents_df):
    """
    Groups the DataFrame by weather condition and counts occurrences.

    This function takes a DataFrame with accident data, groups it by the 'Weather_Condition' column,
    and counts the number of occurrences for each weather condition.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with a 'Weather_Condition' column.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the 'Weather_Condition' column with the count of occurrences for each condition.
    """
    # Extract the date part from 'Start_Time'
    weather_condition_counts_df = usa_accidents_df.withColumn('Date', to_date(col('Start_Time')))

    # Group by the 'Weather_Condition' column and count occurrences
    weather_condition_counts_df = weather_condition_counts_df.groupBy('Weather_Condition', 'Date', 'County', 'State',
                                                                      'Country').count()

    # Select the 'Weather_Condition' column along with the count
    weather_condition_counts_df = weather_condition_counts_df.select('Weather_Condition', 'Date', 'County', 'State',
                                                                     'Country', 'count')

    return weather_condition_counts_df


def group_true_false_columns(usa_accidents_df):
    """
    Groups the DataFrame by several true/false columns and counts occurrences.

    This function takes a DataFrame with accident data and groups it by the specified
    true/false columns, then counts the number of occurrences for each combination of these columns.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with true/false columns.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the specified true/false columns with the count of occurrences for each group.
    """
    # List of true/false columns to group by
    columns = [
        "Amenity",
        "Bump",
        "Crossing",
        "Give_Way",
        "Junction",
        "No_Exit",
        "Railway",
        "Roundabout",
        "Station",
        "Stop",
        "Traffic_Calming",
        "Traffic_Signal",
        "Turning_Loop"
    ]

    # Extract the date part from 'Start_Time'
    true_false_columns_df = usa_accidents_df.withColumn('Date', to_date(col('Start_Time')))

    # Group by the true/false columns and count occurrences
    true_false_columns_df = true_false_columns_df.groupBy(*columns, 'Date', 'County', 'State',
                                                          'Country').count()

    # Select the columns along with the count
    true_false_columns_df = true_false_columns_df.select(*columns, 'Date', 'County', 'State', 'Country', "count")

    return true_false_columns_df


def group_day_night_columns(usa_accidents_df):
    """
    Groups the DataFrame by day/night-related columns and counts occurrences.

    This function takes a DataFrame with accident data and groups it by the specified
    day/night-related columns, then counts the number of occurrences for each combination of these columns.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with day/night-related columns.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the specified day/night-related columns with the count of occurrences for each group.
    """
    # List of day/night-related columns to group by
    day_night_columns = [
        "Sunrise_Sunset",
        "Civil_Twilight",
        "Nautical_Twilight",
        "Astronomical_Twilight"
    ]

    # Extract the date part from 'Start_Time'
    day_night_df = usa_accidents_df.withColumn('Date', to_date(col('Start_Time')))

    # Group by the day/night-related columns and count occurrences
    day_night_df = day_night_df.groupBy(*day_night_columns, 'Date', 'County', 'State',
                                        'Country').count()

    # Select the day/night-related columns along with the count
    day_night_df = day_night_df.select(*day_night_columns, 'Date', 'County', 'State', 'Country', "count")

    return day_night_df


def group_humidity(usa_accidents_df, humidity_column="Humidity_%", labeled_humidity_column="humidity_label"):
    """
    Groups and labels humidity percentages in the given DataFrame and counts the occurrences of each humidity range.

    Parameters:
    usa_accidents_df (DataFrame): The input DataFrame containing the accident data with a humidity column.
    humidity_column (str): The name of the column in the DataFrame that contains humidity percentages. Default is
    "Humidity(%)".

    labeled_humidity_column (str): The name of the new column to be created that will contain the labeled humidity
    ranges. Default is "humidity_label".

    Returns:
    DataFrame: A DataFrame with two columns: 'humidity_label' and 'count', where 'humidity_label' contains the humidity
    ranges and 'count' contains the number of occurrences for each range.
    """
    grouped_humidity_df = usa_accidents_df.withColumn(
        labeled_humidity_column,
        when((col(humidity_column) >= 0) & (col(humidity_column) <= 10), '0-10 %')
        .when((col(humidity_column) > 10) & (col(humidity_column) <= 20), '10-20 %')
        .when((col(humidity_column) > 20) & (col(humidity_column) <= 30), '20-30 %')
        .when((col(humidity_column) > 30) & (col(humidity_column) <= 40), '30-40 %')
        .when((col(humidity_column) > 40) & (col(humidity_column) <= 50), '40-50 %')
        .when((col(humidity_column) > 50) & (col(humidity_column) <= 60), '50-60 %')
        .when((col(humidity_column) > 60) & (col(humidity_column) <= 70), '60-70 %')
        .when((col(humidity_column) > 70) & (col(humidity_column) <= 80), '70-80 %')
        .when((col(humidity_column) > 80) & (col(humidity_column) <= 90), '80-90 %')
        .when((col(humidity_column) > 90) & (col(humidity_column) <= 100), '90-100 %')
        .when((col(humidity_column).isNull()), np.nan)
    )

    # Extract the date part from 'Start_Time'
    grouped_humidity_df = grouped_humidity_df.withColumn('Date', to_date(col('Start_Time')))

    grouped_humidity_df = grouped_humidity_df.groupBy(labeled_humidity_column, 'Date', 'County', 'State', 'Country') \
        .count()

    grouped_humidity_df = grouped_humidity_df.select(labeled_humidity_column, 'Date', 'County', 'State', 'Country',
                                                     'count')

    return grouped_humidity_df


def group_wind_speed(
        usa_accidents_df, wind_speed_column="Wind_Speed_mph", labeled_wind_speed_column="wind_speed_label"
):
    """
    Groups and labels wind speed in the given DataFrame after converting from miles per hour (mph) to kilometers
    per hour (kmph), and counts the occurrences of each wind speed range.

    Parameters:
    usa_accidents_df (DataFrame): The input DataFrame containing the accident data with a wind speed column.
    wind_speed_column (str): The name of the column in the DataFrame that contains wind speed in miles per hour.
    Default is "Wind_Speed(mph)".

    labeled_wind_speed_column (str): The name of the new column to be created that will contain the labeled wind speed
    ranges. Default is "wind_speed_label".

    Returns:
    DataFrame: A DataFrame with two columns: 'wind_speed_label' and 'count', where 'wind_speed_label' contains the wind
    speed ranges (converted to kmph) and 'count' contains the number of occurrences for each range.
    """
    # Transform miles per hours to kilometers per hours
    grouped_wind_speed_df = usa_accidents_df.withColumn(
        wind_speed_column, col(wind_speed_column) * 1.60934)
    grouped_wind_speed_df = grouped_wind_speed_df.withColumn(
        labeled_wind_speed_column,
        when((col(wind_speed_column) >= 0) & (col(wind_speed_column) <= 5), '0-5 kmph')
        .when((col(wind_speed_column) > 5) & (col(wind_speed_column) <= 10), '5-10 kmph')
        .when((col(wind_speed_column) > 10) & (col(wind_speed_column) <= 15), '10-15 kmph')
        .when((col(wind_speed_column) > 15) & (col(wind_speed_column) <= 20), '15-20 kmph')
        .when((col(wind_speed_column) > 20) & (col(wind_speed_column) <= 25), '20-25 kmph')
        .when((col(wind_speed_column) > 25) & (col(wind_speed_column) <= 30), '25-30 kmph')
        .when((col(wind_speed_column) > 25) & (col(wind_speed_column) <= 30), '25-30 kmph')

        .when((col(wind_speed_column).isNull()), np.nan)
        .otherwise('+30 kmph')  # Just in case there are values outside the expected range
    )

    # Extract the date part from 'Start_Time'
    grouped_wind_speed_df = grouped_wind_speed_df.withColumn('Date', to_date(col('Start_Time')))

    grouped_wind_speed_df = grouped_wind_speed_df.groupBy(labeled_wind_speed_column, 'Date', 'County', 'State',
                                                          'Country') \
        .count()

    grouped_wind_speed_df = grouped_wind_speed_df.select(labeled_wind_speed_column, 'Date', 'County', 'State',
                                                         'Country', 'count')

    return grouped_wind_speed_df


import pyspark
from pyspark.sql.functions import col, to_date, concat_ws, when, hour
import numpy as np

def extract_date(df, date_column='Start_Time'):
    """
    Helper function to extract the date from a datetime column.
    """
    return df.withColumn('Date', to_date(col(date_column)))

def sum_accidents_per_hour(usa_accidents_df):
    """
    Create aggregate for the number of accidents per hour.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Extracted table of accidents from CSV converted to PySpark DataFrame.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Table of number of accidents per day per hour.
    """
    accidents_per_hour = extract_date(usa_accidents_df)
    accidents_per_hour = accidents_per_hour.withColumn('Hour', hour(col('Start_Time')))
    accidents_per_hour = accidents_per_hour.groupBy('Date', 'Hour', 'County', 'State', 'Country').count()
    return accidents_per_hour.select('Date', 'Hour', 'County', 'State', 'Country', 'count')

def sum_accidents_per_location_and_day(usa_accidents_df):
    """
    Create aggregate for the number of accidents per day per location.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Extracted table of accidents from CSV converted to PySpark DataFrame.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Table of number of accidents per day per location.
    """
    num_of_accidents = extract_date(usa_accidents_df)
    num_of_accidents = num_of_accidents.groupBy('Date', 'County', 'State', 'Country').count()
    num_of_accidents = num_of_accidents.withColumn(
        'Address', concat_ws(', ', col('City'), col('County'), col('State'), col('Country')))
    return num_of_accidents.select('Date', 'Address', 'City', 'State', 'count')

def group_visibility(usa_accidents_df, visibility_column="Visibility_mi", labeled_visibility_column="visibility_label"):
    """
    Group visibility data into labeled ranges and count occurrences.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data.
    visibility_column : str
        The name of the column in `usa_accidents_df` that contains the visibility values in miles.
    labeled_visibility_column : str
        The name of the new column to be created in the DataFrame with labeled visibility ranges.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with labeled visibility ranges and the count of occurrences in each range.
    """
    grouped_visibility_df = usa_accidents_df.withColumn(
        visibility_column, col(visibility_column) * 1.60934)
    grouped_visibility_df = grouped_visibility_df.withColumn(
        labeled_visibility_column,
        when((col(visibility_column) >= 0) & (col(visibility_column) <= 1), '0-1 km')
        .when((col(visibility_column) > 1) & (col(visibility_column) <= 2), '1-2 km')
        .when((col(visibility_column) > 2) & (col(visibility_column) <= 3), '2-3 km')
        .when((col(visibility_column) > 3) & (col(visibility_column) <= 4), '3-4 km')
        .when((col(visibility_column) > 4) & (col(visibility_column) <= 5), '4-5 km')
        .when((col(visibility_column) > 5) & (col(visibility_column) <= 6), '5-6 km')
        .when((col(visibility_column) > 6) & (col(visibility_column) <= 7), '6-7 km')
        .when((col(visibility_column) > 7) & (col(visibility_column) <= 8), '7-8 km')
        .when((col(visibility_column) > 8) & (col(visibility_column) <= 9), '8-9 km')
        .when((col(visibility_column) > 9) & (col(visibility_column) <= 10), '9-10 km')
        .when((col(visibility_column).isNull()), np.nan)
        .otherwise('+10 km'))
    grouped_visibility_df = extract_date(grouped_visibility_df)
    grouped_visibility_df = grouped_visibility_df.groupBy(labeled_visibility_column, 'Date', 'County', 'State', 'Country').count()
    return grouped_visibility_df.select(labeled_visibility_column, 'count')

def group_weather_condition(usa_accidents_df):
    """
    Group the DataFrame by weather condition and count occurrences.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with a 'Weather_Condition' column.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the 'Weather_Condition' column with the count of occurrences for each condition.
    """
    weather_condition_counts_df = extract_date(usa_accidents_df)
    weather_condition_counts_df = weather_condition_counts_df.groupBy('Weather_Condition', 'Date', 'County', 'State', 'Country').count()
    return weather_condition_counts_df.select('Weather_Condition', 'Date', 'County', 'State', 'Country', 'count')

def group_true_false_columns(usa_accidents_df):
    """
    Group the DataFrame by several true/false columns and count occurrences.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with true/false columns.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the specified true/false columns with the count of occurrences for each group.
    """
    columns = [
        "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
        "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
        "Traffic_Signal", "Turning_Loop"
    ]
    true_false_columns_df = extract_date(usa_accidents_df)
    true_false_columns_df = true_false_columns_df.groupBy(*columns, 'Date', 'County', 'State', 'Country').count()
    return true_false_columns_df.select(*columns, 'Date', 'County', 'State', 'Country', 'count')

def group_day_night_columns(usa_accidents_df):
    """
    Group the DataFrame by day/night-related columns and count occurrences.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input PySpark DataFrame containing accident data with day/night-related columns.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame grouped by the specified day/night-related columns with the count of occurrences for each group.
    """
    day_night_columns = ["Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]
    day_night_df = extract_date(usa_accidents_df)
    day_night_df = day_night_df.groupBy(*day_night_columns, 'Date', 'County', 'State', 'Country').count()
    return day_night_df.select(*day_night_columns, 'Date', 'County', 'State', 'Country', 'count')

def group_humidity(usa_accidents_df, humidity_column="Humidity_%", labeled_humidity_column="humidity_label"):
    """
    Group and label humidity percentages in the DataFrame and count occurrences of each humidity range.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input DataFrame containing the accident data with a humidity column.
    humidity_column : str
        The name of the column in the DataFrame that contains humidity percentages.
    labeled_humidity_column : str
        The name of the new column to be created that will contain the labeled humidity ranges.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with labeled humidity ranges and the count of occurrences in each range.
    """
    grouped_humidity_df = usa_accidents_df.withColumn(
        labeled_humidity_column,
        when((col(humidity_column) >= 0) & (col(humidity_column) <= 10), '0-10 %')
        .when((col(humidity_column) > 10) & (col(humidity_column) <= 20), '10-20 %')
        .when((col(humidity_column) > 20) & (col(humidity_column) <= 30), '20-30 %')
        .when((col(humidity_column) > 30) & (col(humidity_column) <= 40), '30-40 %')
        .when((col(humidity_column) > 40) & (col(humidity_column) <= 50), '40-50 %')
        .when((col(humidity_column) > 50) & (col(humidity_column) <= 60), '50-60 %')
        .when((col(humidity_column) > 60) & (col(humidity_column) <= 70), '60-70 %')
        .when((col(humidity_column) > 70) & (col(humidity_column) <= 80), '70-80 %')
        .when((col(humidity_column) > 80) & (col(humidity_column) <= 90), '80-90 %')
        .when((col(humidity_column) > 90) & (col(humidity_column) <= 100), '90-100 %')
        .otherwise('Unknown'))
    grouped_humidity_df = extract_date(grouped_humidity_df)
    grouped_humidity_df = grouped_humidity_df.groupBy(labeled_humidity_column, 'Date', 'County', 'State', 'Country').count()
    return grouped_humidity_df.select(labeled_humidity_column, 'Date', 'County', 'State', 'Country', 'count')

def group_wind_speed(usa_accidents_df, wind_speed_column="Wind_Speed_mph", labeled_wind_speed_column="wind_speed_label"):
    """
    Group and label wind speeds in the DataFrame and count occurrences of each wind speed range.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        Input DataFrame containing the accident data with a wind speed column.
    wind_speed_column : str
        The name of the column in the DataFrame that contains wind speeds in mph.
    labeled_wind_speed_column : str
        The name of the new column to be created that will contain the labeled wind speed ranges.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with labeled wind speed ranges and the count of occurrences in each range.
    """
    grouped_wind_speed_df = usa_accidents_df.withColumn(
        labeled_wind_speed_column,
        when((col(wind_speed_column) >= 0) & (col(wind_speed_column) <= 1), '0-1 mph')
        .when((col(wind_speed_column) > 1) & (col(wind_speed_column) <= 2), '1-2 mph')
        .when((col(wind_speed_column) > 2) & (col(wind_speed_column) <= 3), '2-3 mph')
        .when((col(wind_speed_column) > 3) & (col(wind_speed_column) <= 4), '3-4 mph')
        .when((col(wind_speed_column) > 4) & (col(wind_speed_column) <= 5), '4-5 mph')
        .when((col(wind_speed_column) > 5) & (col(wind_speed_column) <= 6), '5-6 mph')
        .when((col(wind_speed_column) > 6) & (col(wind_speed_column) <= 7), '6-7 mph')
        .when((col(wind_speed_column) > 7) & (col(wind_speed_column) <= 8), '7-8 mph')
        .when((col(wind_speed_column) > 8) & (col(wind_speed_column) <= 9), '8-9 mph')
        .when((col(wind_speed_column) > 9) & (col(wind_speed_column) <= 10), '9-10 mph')
        .otherwise('10+ mph'))
    grouped_wind_speed_df = extract_date(grouped_wind_speed_df)
    grouped_wind_speed_df = grouped_wind_speed_df.groupBy(labeled_wind_speed_column, 'Date', 'County', 'State', 'Country').count()
    return grouped_wind_speed_df.select(labeled_wind_speed_column, 'Date', 'County', 'State', 'Country', 'count')


def group_temperature(usa_accidents_df, temperature_column="Temperature_F", labeled_temperature_column="temperature_label"):
    """
    Groups and labels temperature in the given DataFrame after converting from Fahrenheit to Celsius,
    and counts the occurrences of each temperature range.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        The input DataFrame containing the accident data with a temperature column.
    temperature_column : str
        The name of the column in the DataFrame that contains temperature in Fahrenheit. Default is "Temperature_F".
    labeled_temperature_column : str
        The name of the new column to be created that will contain the labeled temperature ranges. Default is "temperature_label".

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with two columns: 'temperature_label' and 'count', where 'temperature_label' contains the temperature ranges (converted to Celsius) and 'count' contains the number of occurrences for each range.
    """
    # Convert Fahrenheit to Celsius and label temperature ranges
    grouped_temperature_df = usa_accidents_df.withColumn(
        temperature_column, (col(temperature_column) - 32) / 1.8
    ).withColumn(
        labeled_temperature_column,
        when(col(temperature_column) <= -20, '>(-)20 C')
        .when((col(temperature_column) > -20) & (col(temperature_column) <= -15), '(-)20-(-)15 C')
        .when((col(temperature_column) > -15) & (col(temperature_column) <= -10), '(-)15-(-)10 C')
        .when((col(temperature_column) > -10) & (col(temperature_column) <= -5), '(-)10-(-)5 C')
        .when((col(temperature_column) > -5) & (col(temperature_column) <= 0), '(-)5-0 C')
        .when((col(temperature_column) >= 0) & (col(temperature_column) <= 5), '0-5 C')
        .when((col(temperature_column) > 5) & (col(temperature_column) <= 10), '5-10 C')
        .when((col(temperature_column) > 10) & (col(temperature_column) <= 15), '10-15 C')
        .when((col(temperature_column) > 15) & (col(temperature_column) <= 20), '15-20 C')
        .when((col(temperature_column) > 20) & (col(temperature_column) <= 25), '20-25 C')
        .when((col(temperature_column) > 25) & (col(temperature_column) <= 30), '25-30 C')
        .when((col(temperature_column) > 30) & (col(temperature_column) <= 35), '30-35 C')
        .when((col(temperature_column) > 35) & (col(temperature_column) <= 40), '35-40 C')
        .when((col(temperature_column) > 40) & (col(temperature_column) <= 45), '40-45 C')
        .otherwise('+45 C')
    )

    # Extract the date part from 'Start_Time' and count occurrences
    grouped_temperature_df = extract_date(grouped_temperature_df)
    grouped_temperature_df = grouped_temperature_df.groupBy(labeled_temperature_column, 'Date', 'County', 'State', 'Country').count()

    return grouped_temperature_df.select(labeled_temperature_column, 'Date', 'County', 'State', 'Country', 'count')


def group_precipitation(usa_accidents_df, precipitation_column="Precipitation_in", labeled_precipitation_column="precipitation_label"):
    """
    Groups and labels precipitation amounts in the given DataFrame after converting from inches to centimeters,
    and counts the occurrences of each precipitation range.

    Parameters
    ----------
    usa_accidents_df : pyspark.sql.dataframe.DataFrame
        The input DataFrame containing the accident data with a precipitation column.
    precipitation_column : str
        The name of the column in the DataFrame that contains precipitation amounts in inches. Default is "Precipitation_in".
    labeled_precipitation_column : str
        The name of the new column to be created that will contain the labeled precipitation ranges. Default is "precipitation_label".

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        DataFrame with two columns: 'precipitation_label' and 'count', where 'precipitation_label' contains the precipitation ranges (converted to centimeters) and 'count' contains the number of occurrences for each range.
    """
    # Convert inches to centimeters and label precipitation ranges
    grouped_precipitation_df = usa_accidents_df.withColumn(
        precipitation_column, col(precipitation_column) * 2.54
    ).withColumn(
        labeled_precipitation_column,
        when((col(precipitation_column) >= 0) & (col(precipitation_column) <= 0.25), '0-0.25 cm')
        .when((col(precipitation_column) > 0.25) & (col(precipitation_column) <= 0.5), '0.25-0.5 cm')
        .when((col(precipitation_column) > 0.5) & (col(precipitation_column) <= 0.75), '0.5-0.75 cm')
        .when((col(precipitation_column) > 0.75) & (col(precipitation_column) <= 1.0), '0.75-1.0 cm')
        .when((col(precipitation_column) > 1.0) & (col(precipitation_column) <= 1.25), '1.0-1.25 cm')
        .when((col(precipitation_column) > 1.25) & (col(precipitation_column) <= 1.5), '1.25-1.5 cm')
        .when((col(precipitation_column) > 1.5) & (col(precipitation_column) <= 1.75), '1.5-1.75 cm')
        .when((col(precipitation_column) > 1.75) & (col(precipitation_column) <= 2.0), '1.75-2.0 cm')
        .when((col(precipitation_column) > 2.0) & (col(precipitation_column) <= 2.25), '2.0-2.25 cm')
        .when((col(precipitation_column) > 2.25) & (col(precipitation_column) <= 2.5), '2.25-2.5 cm')
        .when((col(precipitation_column) > 2.5) & (col(precipitation_column) <= 2.75), '2.5-2.75 cm')
        .when((col(precipitation_column) > 2.75) & (col(precipitation_column) <= 3.0), '2.75-3.0 cm')
        .when((col(precipitation_column) > 3.0) & (col(precipitation_column) <= 3.25), '3.0-3.25 cm')
        .when((col(precipitation_column) > 3.25) & (col(precipitation_column) <= 3.5), '3.25-3.5 cm')
        .when((col(precipitation_column) > 3.5) & (col(precipitation_column) <= 3.75), '3.5-3.75 cm')
        .when((col(precipitation_column) > 3.75) & (col(precipitation_column) <= 4.0), '3.75-4.0 cm')
        .when((col(precipitation_column) > 4.0) & (col(precipitation_column) <= 4.25), '4.0-4.25 cm')
        .when((col(precipitation_column) > 4.25) & (col(precipitation_column) <= 4.5), '4.25-4.5 cm')
        .when((col(precipitation_column) > 4.5) & (col(precipitation_column) <= 4.75), '4.5-4.75 cm')
        .when((col(precipitation_column) > 4.75) & (col(precipitation_column) <= 5.0), '4.75-5.0 cm')
        .otherwise('>5.0 cm')
    )

    # Extract the date part from 'Start_Time' and count occurrences
    grouped_precipitation_df = extract_date(grouped_precipitation_df)
    grouped_precipitation_df = grouped_precipitation_df.groupBy(labeled_precipitation_column, 'Date', 'County', 'State', 'Country').count()

    return grouped_precipitation_df.select(labeled_precipitation_column, 'Date', 'County', 'State', 'Country', 'count')
