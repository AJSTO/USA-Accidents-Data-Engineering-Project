CREATE OR REPLACE TABLE `PROJECT_ID.DATASET_ID.TABLE_ID` AS
SELECT
  'humidity' as breakdown,
  humidity_label as label,
  Date,
  County,
  State,
  Country,
  count
 FROM `PROJECT_ID.DATASET_ID.group_humidity`

union all

SELECT
  'precipitation' as breakdown,
  precipitation_label as label,
  Date,
  County,
  State,
  Country,
  count
 FROM `PROJECT_ID.DATASET_ID.group_precipitation`

 union all

SELECT
  'temperature' as breakdown,
  temperature_label as label,
  Date,
  County,
  State,
  Country,
  count
 FROM `PROJECT_ID.DATASET_ID.group_temperature`

 union all

SELECT
  'weather cond.' as breakdown,
  Weather_Condition as label,
  Date,
  County,
  State,
  Country,
  count
 FROM `PROJECT_ID.DATASET_ID.group_weather_condition`

 union all

SELECT
  'wind speed' as breakdown,
  wind_speed_label as label,
  Date,
  County,
  State,
  Country,
  count
 FROM `PROJECT_ID.DATASET_ID.wind_speed_ranges_per_location_and_day`