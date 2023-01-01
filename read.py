from pyspark.sql import SparkSession
from google.cloud import storage


class ReadData:
    """
    A class to read data from json files using spark.

    Attributes:
    spark: SparkSession
        Holds the spark session

    Methods:
    read_data()
        Reads the data from json files using spark
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_data(self):
        """
        Uses Spark to all authority details from cloud storage into a dataframe

        :return:
            dataframe
        """
        try:
            dataframe = self.spark.read.json("gs://hygiene_data/*.json")

        except FileNotFoundError as fnf_error:
            raise Exception(fnf_error)
        return dataframe


