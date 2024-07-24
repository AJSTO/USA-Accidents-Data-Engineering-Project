import logging
from pyspark.sql import SparkSession
from jobs import accidents_job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Main function executed by spark-submit command.

    Creates a Spark session with a connection to BigQuery and runs the ETL job.
    """
    logger.info("Starting Spark session")

    spark = SparkSession.builder \
        .appName('USA_Accidents') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar') \
        .getOrCreate()

    logger.info("Spark session created")

    try:
        logger.info("Running ETL job")
        accidents_job._run_job(spark)
        logger.info("ETL job completed successfully")
    except Exception as e:
        logger.error("ETL job failed", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

