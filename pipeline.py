from pyspark.sql import SparkSession
from read import ReadData
from transform import TransformData
from write import WriteData

class Pipeline:
    """
    A class to manage the data pipeline

    Attributes:
    spark: SparkSession
        Holds the spark session

    Methods:
    execute()
        Runs the data pipeline using ReadData, TransformData and WriteData classes
    """

    def __init__(self):
        self.spark = SparkSession.builder.appName('Hygiene Ratings Cloud Project').getOrCreate()

    def execute(self):
        """
        Executes the data pipeline by instantiating the ReadData, TransformData and WriteData classes
        and using the methods of those classes to achieve the Exctracting, Transforming and Loading
        of the data
        :return:
        """
        try:
            read = ReadData(self.spark)
            hygiene_data_df = read.read_data()

            transform = TransformData()
            extracted_df = transform.extract_relevant_data(hygiene_data_df)
            cleaned_df = transform.clean_data(extracted_df)
            cleaned_df.show()
            transformed_df = transform.perform_aggregations(cleaned_df)
            transformed_df.show()

            write = WriteData(self.spark)
            write.write_data_to_bigquery(transformed_df)
        except Exception as error:
            print(error)








