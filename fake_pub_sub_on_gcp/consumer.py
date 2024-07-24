import base64
import json
import pandas as pd
from google.cloud import bigquery

# Setting Project ID
project_id = 'YOUR_PROJECT_ID'

# Initialize a BigQuery client
client = bigquery.Client(project=project_id)

# Define the BigQuery dataset and table name
dataset_id = 'YOUR_DATASET_NAME'
table_id = 'YOUR_TABLE_NAME'


def load_to_bq(output):
    """
    Load a DataFrame into a BigQuery table with a predefined schema.

    Parameters:
    output (pandas.DataFrame): The DataFrame to be loaded into BigQuery.

    This function performs the following steps:
    1. Defines a job configuration for BigQuery with a specific schema for the target table.
    2. Sets the write disposition to "WRITE_APPEND" which appends the data to the table if it exists.
    3. Loads the DataFrame into the specified BigQuery table.
    4. Waits for the job to complete.
    5. Prints a message indicating the number of rows loaded and the target table details.

    Note:
    Ensure that the BigQuery client (`client`), project ID (`project_id`), dataset ID (`dataset_id`), and table ID (`table_id`)
    are defined in the scope where this function is called.
    """
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ID", "STRING"),
            bigquery.SchemaField("Source", "STRING"),
            bigquery.SchemaField("Severity", "STRING"),
            bigquery.SchemaField("Start_Time", "STRING"),
            bigquery.SchemaField("End_Time", "STRING"),
            bigquery.SchemaField("Start_Lat", "STRING"),
            bigquery.SchemaField("Start_Lng", "STRING"),
            bigquery.SchemaField("End_Lat", "STRING"),
            bigquery.SchemaField("End_Lng", "STRING"),
            bigquery.SchemaField("Distance_mi", "STRING"),
            bigquery.SchemaField("Description", "STRING"),
            bigquery.SchemaField("Street", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("County", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("Zipcode", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Timezone", "STRING"),
            bigquery.SchemaField("Airport_Code", "STRING"),
            bigquery.SchemaField("Weather_Timestamp", "STRING"),
            bigquery.SchemaField("Temperature_F", "STRING"),
            bigquery.SchemaField("Wind_Chill_F", "STRING"),
            bigquery.SchemaField("Humidity_percent", "STRING"),
            bigquery.SchemaField("Pressure_in", "STRING"),
            bigquery.SchemaField("Visibility_mi", "STRING"),
            bigquery.SchemaField("Wind_Direction", "STRING"),
            bigquery.SchemaField("Wind_Speed_mph", "STRING"),
            bigquery.SchemaField("Precipitation_in", "STRING"),
            bigquery.SchemaField("Weather_Condition", "STRING"),
            bigquery.SchemaField("Amenity", "STRING"),
            bigquery.SchemaField("Bump", "STRING"),
            bigquery.SchemaField("Crossing", "STRING"),
            bigquery.SchemaField("Give_Way", "STRING"),
            bigquery.SchemaField("Junction", "STRING"),
            bigquery.SchemaField("No_Exit", "STRING"),
            bigquery.SchemaField("Railway", "STRING"),
            bigquery.SchemaField("Roundabout", "STRING"),
            bigquery.SchemaField("Station", "STRING"),
            bigquery.SchemaField("Stop", "STRING"),
            bigquery.SchemaField("Traffic_Calming", "STRING"),
            bigquery.SchemaField("Traffic_Signal", "STRING"),
            bigquery.SchemaField("Turning_Loop", "STRING"),
            bigquery.SchemaField("Sunrise_Sunset", "STRING"),
            bigquery.SchemaField("Civil_Twilight", "STRING"),
            bigquery.SchemaField("Nautical_Twilight", "STRING"),
            bigquery.SchemaField("Astronomical_Twilight", "STRING"),
        ],
        write_disposition="WRITE_APPEND",  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    )

    job = client.load_table_from_dataframe(
        output, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
    )

    job.result()  # Wait for the job to complete

    print(f"Loaded {len(pd.DataFrame(output))} rows into {project_id}:{dataset_id}.{table_id}")


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print("This Function was triggered by messageId {} published at {}".format(context.event_id, context.timestamp))

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
        row = json.loads(name)
        row = [row]
        record = pd.DataFrame(row)
        record = record.applymap(str)
        print(record)
    print('"{}" received!'.format(name))
    load_to_bq(record)

    if 'attributes' in event:
        print(event['attributes'])

    if '@type' in event:
        print(event['@type'])
