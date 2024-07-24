import os
import json
import random
from google.cloud import pubsub_v1
from datetime import datetime, timedelta

# Initialize the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
project_id = 'YOUR_PROJECT_ID'  # Replace with your project ID
topic_name = 'YOUR_TOPIC_NAME'    # Replace with your topic name
topic_path = publisher.topic_path(project_id, topic_name)


def generate_sample_data():
    """
    Generates a sample dictionary representing accident data with random values.

    This function creates a dictionary with randomly generated accident data for testing purposes.
    The generated data includes various attributes such as ID, source, severity, timestamps, location,
    weather conditions, and other relevant information.
    """
    start_time = datetime.now() - timedelta(hours=random.randint(1, 3))
    end_time = start_time + timedelta(minutes=random.randint(30, 120))

    data = {
        "ID": f"A-{random.randint(1, 1000)}",
        "Source": random.choice(["Source1", "Source2"]),
        "Severity": random.randint(1, 4),
        "Start_Time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "End_Time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
        "Start_Lat": random.uniform(-90.0, 90.0),
        "Start_Lng": random.uniform(-180.0, 180.0),
        "End_Lat": random.uniform(-90.0, 90.0),
        "End_Lng": random.uniform(-180.0, 180.0),
        "Distance_mi": round(random.uniform(0, 50), 2),
        "Description": "Sample description",
        "Street": "Sample Street",
        "City": "Sample City",
        "County": "Sample County",
        "State": "Sample State",
        "Zipcode": "12345",
        "Country": "US",
        "Timezone": "US/Eastern",
        "Airport_Code": "SAMPLE",
        "Weather_Timestamp": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "Temperature_F": round(random.uniform(-20, 120), 1),
        "Wind_Chill_F": round(random.uniform(-30, 50), 1),
        "Humidity_percent": round(random.uniform(0, 100), 1),
        "Pressure_in": round(random.uniform(29.0, 31.0), 2),
        "Visibility_mi": round(random.uniform(0, 10), 1),
        "Wind_Direction": random.choice(["N", "S", "E", "W"]),
        "Wind_Speed_mph": round(random.uniform(0, 100), 1),
        "Precipitation_in": round(random.uniform(0, 10), 2),
        "Weather_Condition": "Clear",
        "Amenity": random.choice([True, False]),
        "Bump": random.choice([True, False]),
        "Crossing": random.choice([True, False]),
        "Give_Way": random.choice([True, False]),
        "Junction": random.choice([True, False]),
        "No_Exit": random.choice([True, False]),
        "Railway": random.choice([True, False]),
        "Roundabout": random.choice([True, False]),
        "Station": random.choice([True, False]),
        "Stop": random.choice([True, False]),
        "Traffic_Calming": random.choice([True, False]),
        "Traffic_Signal": random.choice([True, False]),
        "Turning_Loop": random.choice([True, False]),
        "Sunrise_Sunset": random.choice(["Day", "Night"]),
        "Civil_Twilight": random.choice(["Day", "Night"]),
        "Nautical_Twilight": random.choice(["Day", "Night"]),
        "Astronomical_Twilight": random.choice(["Day", "Night"])

    }

    return data


def publish_message(request):
    """
    HTTP Cloud Function to generate sample accident data and publish it to a Google Cloud Pub/Sub topic.

    This function performs the following steps:
    1. Generates sample accident data using the `generate_sample_data` function.
    2. Converts the generated data to a JSON string and encodes it to bytes.
    3. Publishes the encoded message to a specified Pub/Sub topic.
    4. Waits for the publish operation to complete.
    5. Returns an HTTP response indicating success or failure.

    Parameters:
    request (flask.Request): The request object containing HTTP request data. This parameter is required by Google Cloud Functions,
                             but is not used in the current implementation.

    Returns:
    tuple: A tuple containing a string message and an HTTP status code.
           "OK" with status code 200 if the message is successfully published.
           "Not OK" with status code 500 if there is an error during the process.

    Raises:
    Exception: If any error occurs during message generation, encoding, or publishing, the exception is caught and an error message
               is printed, followed by returning "Not OK" with a 500 status code.

    Example Usage:
    This function is intended to be deployed as a Google Cloud Function and triggered via an HTTP request.

    Note:
    - Ensure that the Google Cloud Pub/Sub client (`publisher`), project ID (`project_id`), and topic name (`topic_name`) are defined
      and correctly configured in the scope where this function is deployed.
    - The `generate_sample_data` function is used to create the sample accident data.

    Example Request:
    An HTTP request to trigger this function does not require any specific data and can be a simple GET request.

    Example Response:
    - "OK", 200 if the data is successfully published to Pub/Sub.
    - "Not OK", 500 if an error occurs during the process.
    """
    try:
        # Generate sample accident data
        accident_data = generate_sample_data()

        # Convert data to JSON and encode to bytes
        message = json.dumps(accident_data).encode('utf-8')

        # Publish the message to the Pub/Sub topic
        future = publisher.publish(topic_path, message)
        future.result()  # Wait for the publish to complete

        return "OK", 200
    except Exception as e:
        print(f"Error: {e}")
        return "Not OK", 500

