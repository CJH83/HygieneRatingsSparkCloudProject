from pyspark.sql import SparkSession

class WriteData:
    """
    A class to write a spark dataframe Google BigQuery.

    Attributes:
    spark: SparKSession
        Holds the spark session
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_data_to_bigquery(self, dataframe):
        """
        Writes a spark dataframe to Google BigQuery

        :param dataframe:
        """
        try:
            dataframe.write.format('bigquery') \
                .option('table', 'hygiene_data.hygiene_ratings') \
                .option('parentProject', 'dynamic-parity-372821') \
                .option('temporaryGcsBucket', 'bigquery-temp-1') \
                .mode("append") \
                .save()

        except Exception as error:
            print(error)
            raise Exception('Could not write to BigQuery')

