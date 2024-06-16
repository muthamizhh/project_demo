import luigi
import splunklib.client as splunk_client
import splunklib.results as splunk_results
from pymongo import MongoClient
import json
import time
import os
import logging.config
import configparser

configuration = configparser.ConfigParser()
configuration.read('/Users/muthamizh/PycharmProjects/datapipeline/config/config.ini')
# print(configuration['splunk']['host'])
script_dir = os.path.dirname(os.path.abspath(__file__))
print(script_dir)
log_path = os.path.join(script_dir,"logging.ini")
logging.config.fileConfig(log_path)

logger = logging.getLogger('luigi-interface')

# print("hello")

# ini_file = os.path.join(script_dir,"logging.ini")
# log_file = os.path.join(script_dir,"logfile.log")
#
# print(log_file)
# logging.config.fileConfig(ini_file,defaults ={"log_file_path":log_file})
# logger = logging.getLogger('luigi-interface')
# logger.debug("hello world")
class ExtractSplunkData(luigi.Task):
    splunk_host = luigi.Parameter()
    splunk_port = luigi.IntParameter()
    splunk_username = luigi.Parameter()
    splunk_password = luigi.Parameter()
    splunk_query = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("splunk_data.json")

    def run(self):
        try:
            logger.info("Connecting to Splunk")
            service = splunk_client.connect(
                host=self.splunk_host,
                port=self.splunk_port,
                username=self.splunk_username,
                password=self.splunk_password
            )

            logger.info("Running Splunk query")
            job = service.jobs.create(self.splunk_query)

            logger.info("Waiting for job to complete")
            while not job.is_done():
                logger.debug("Job not done yet, waiting...")
                time.sleep(1)

            logger.info("Retrieving results")
            results = job.results()
            data = splunk_results.ResultsReader(results)

            logger.info("Writing results to JSON file")
            with self.output().open("w") as f:
                json.dump([event for event in data], f, indent=4)

            logger.info("Successfully wrote results to JSON file")

        except Exception as e:
            logger.error(f"Failed to extract data from Splunk: {e}")

    def complete(self):
        return os.path.exists(self.output().path) and os.path.getsize(self.output().path) > 0

class LoadDataToMongoDB(luigi.Task):
    mongodb_host = luigi.Parameter()
    mongodb_port = luigi.IntParameter()
    mongodb_db = luigi.Parameter()
    mongodb_collection = luigi.Parameter()

    def requires(self):
        return ExtractSplunkData(
            # splunk_host="localhost",
            splunk_host=configuration['splunk']['host'],
            splunk_port=int(configuration['splunk']['port']),
            splunk_username="Muthamizh",
            splunk_password="Rocky@001",
            splunk_query='search index=_internal | head 10'
        )

    def run(self):
        logger.info("Connecting to MongoDB")
        client = MongoClient(self.mongodb_host, self.mongodb_port)
        db = client[self.mongodb_db]
        collection = db[self.mongodb_collection]

        logger.info("Reading data from JSON file")
        try:
            with self.input().open("r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            return

        if not data:
            logger.warning("No data to insert into MongoDB")
            return

        logger.info("Inserting data into MongoDB")
        collection.insert_many(data)

    def complete(self):
        return False  # Ensure this task runs every time

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    luigi.run()
