# Setting Pub/Sub on GCP

## 1. Set up Pub/Sub Subscription

[What is Pub/Sub?](https://cloud.google.com/pubsub/docs/overview)

First, you need to set up a Pub/Sub subscription to your my-topic topic where messages are published. You can do this via Google Cloud Console or using gcloud command-line tool:

```bash 
gcloud pubsub subscriptions create my-subscription --topic=my-topic
```

## 2. Create a Cloud Function which will produce data by using the Google Cloud CLI:

[Create a Cloud Function by using the Google Cloud CLI | Cloud Functions Documentation](https://cloud.google.com/functions/docs/create-deploy-gcloud)

#### 2.1. Create a Python file `main.py` with the content from file `producer.py`

#### 2.2. Create a `requirements.txt` with the content from file `requirements_producer.py`

#### 2.3. Deploy the Cloud Function
Deploy the function using the gcloud command:

```sh
gcloud functions deploy publish_message \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point publish_message
```
During deployment, make sure to select the HTTPS trigger option. This will allow the function to be triggered via HTTP requests.

#### 2.4. Test the Cloud Function
After deploying the function, you can test it by sending an HTTP request using curl or any HTTP client:

```sh
curl https://REGION-PROJECT_ID.cloudfunctions.net/publish_message
```
Replace REGION and PROJECT_ID with the appropriate values from your deployment.

#### 2.5. Run the Function Locally with a Script
To run the function locally every 20 seconds, create a script run_local.sh with the following content:

```sh
#!/bin/bash

# URL of the deployed Cloud Function
FUNCTION_URL="https://REGION-PROJECT_ID.cloudfunctions.net/publish_message"

while true; do
  # Send an HTTP request to the Cloud Function
  curl -X GET $FUNCTION_URL
  # Wait for 20 seconds
  sleep 20
done
```

Make the script executable:

```sh
chmod +x run_local.sh
```
Run the script:

```sh
./run_local.sh
```
This script will send an HTTP request to the deployed Cloud Function every 20 seconds.

## 3. Create a Cloud Function which will consume data by using the Google Cloud CLI:

#### 3.1. Set Up BigQuery Dataset and Table

Before creating the Cloud Function, ensure that you have a BigQuery dataset and table ready to receive the data. You can create a dataset and table via the Google Cloud Console or using the `bq` command-line tool.

```bash
bq mk YOUR_DATASET_NAME
```

#### 3.2. Create a BigQuery Table
Define the schema for the table using a JSON file (schema.json). Here's an example schema:

```json
[
    {"name": "ID", "type": "STRING"},
    {"name": "Source", "type": "STRING"},
    {"name": "Severity", "type": "STRING"},
    {"name": "Start_Time", "type": "STRING"},
    {"name": "End_Time", "type": "STRING"},
    {"name": "Start_Lat", "type": "STRING"},
    {"name": "Start_Lng", "type": "STRING"},
    {"name": "End_Lat", "type": "STRING"},
    {"name": "End_Lng", "type": "STRING"},
    {"name": "Distance_mi", "type": "STRING"},
    {"name": "Description", "type": "STRING"},
    {"name": "Street", "type": "STRING"},
    {"name": "City", "type": "STRING"},
    {"name": "County", "type": "STRING"},
    {"name": "State", "type": "STRING"},
    {"name": "Zipcode", "type": "STRING"},
    {"name": "Country", "type": "STRING"},
    {"name": "Timezone", "type": "STRING"},
    {"name": "Airport_Code", "type": "STRING"},
    {"name": "Weather_Timestamp", "type": "STRING"},
    {"name": "Temperature_F", "type": "STRING"},
    {"name": "Wind_Chill_F", "type": "STRING"},
    {"name": "Humidity_percent", "type": "STRING"},
    {"name": "Pressure_in", "type": "STRING"},
    {"name": "Visibility_mi", "type": "STRING"},
    {"name": "Wind_Direction", "type": "STRING"},
    {"name": "Wind_Speed_mph", "type": "STRING"},
    {"name": "Precipitation_in", "type": "STRING"},
    {"name": "Weather_Condition", "type": "STRING"},
    {"name": "Amenity", "type": "STRING"},
    {"name": "Bump", "type": "STRING"},
    {"name": "Crossing", "type": "STRING"},
    {"name": "Give_Way", "type": "STRING"},
    {"name": "Junction", "type": "STRING"},
    {"name": "No_Exit", "type": "STRING"},
    {"name": "Railway", "type": "STRING"},
    {"name": "Roundabout", "type": "STRING"},
    {"name": "Station", "type": "STRING"},
    {"name": "Stop", "type": "STRING"},
    {"name": "Traffic_Calming", "type": "STRING"},
    {"name": "Traffic_Signal", "type": "STRING"},
    {"name": "Turning_Loop", "type": "STRING"},
    {"name": "Sunrise_Sunset", "type": "STRING"},
    {"name": "Civil_Twilight", "type": "STRING"},
    {"name": "Nautical_Twilight", "type": "STRING"},
    {"name": "Astronomical_Twilight", "type": "STRING"}
]
```

Create the table using the `bq` command:

```bash
bq mk --table --schema schema.json YOUR_DATASET_NAME.YOUR_TABLE_NAME
```

#### 3.3. Create a Python file `main.py` with the content from file `consumer.py`

#### 3.4. Create a `requirements.txt` with the content from file `requirements_consumer.py`

#### 3.5. Deploy the Cloud Function

```bash
gcloud functions deploy hello_pubsub \
  --runtime python39 \
  --trigger-topic YOUR_TOPIC_NAME \
  --entry-point hello_pubsub
```

#### 3.6. Test the Cloud Function
To test the Cloud Function, publish a message to the Pub/Sub topic:

```sh
gcloud pubsub topics publish YOUR_TOPIC_NAME --message '{"ID":"A-123", "Source":"Source1", "Severity":"High", "Start_Time":"2024-07-23 12:00:00", "End_Time":"2024-07-23 13:00:00", "Start_Lat":"34.05", "Start_Lng":"-118.24"}'
```
Replace the message content with appropriate sample data.