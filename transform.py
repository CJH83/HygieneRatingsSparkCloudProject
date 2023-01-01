from pyspark.sql import SparkSession

class TransformData:
    """
    A class to transform data using spark dataframes

    Methods:

    extract_relevant_data(dataframe)
        returns a dataframe with only the relevant information

    clean_data(dataframe)
        drops records where the 'RatingValue' is either 'AwatingInspection' or 'Exempt'

    perform_aggregations(dataframe)
        performs the necessary aggregations. See eac method for more detail
    """



    def extract_relevant_data(self, dataframe):
        """
        Extracts the relevant data which is to be used from a dataframe
        :param dataframe:
        :return: dataframe:
                With the BuisnessName, RatingValue and LocalAuthority information
        """
        return dataframe.select('BusinessName', 'RatingValue', 'LocalAuthorityName')

    def clean_data(self, dataframe):
        """
        Drops rows where the RatingValue value is either 'AwaitingInspection' or 'Exempt'
        :param dataframe:
        :return: dataframe:
        """
        return dataframe.filter(dataframe.RatingValue != 'AwaitingInspection') \
                                .filter(dataframe.RatingValue != 'Exempt')

    def perform_aggregations(self, dataframe):
        """
        Calculates the number of businseses for each LocalAuthority.
        Calculates the number of each rating for each business.
        Joins the two dataframes on the LocalAuthorityName
        Calculates the percentages of each hygiene rating
        :param dataframe:
        :return: dataframe: With calculated percentages of each rating
        """
        authority_count_df = dataframe.groupBy('LocalAuthorityName').count().withColumnRenamed('count', 'TotalBusinesses')
        rating_count_df = dataframe.groupBy('RatingValue', 'LocalAuthorityName').count().withColumnRenamed('count', 'TotalRatings')
        totals_df = rating_count_df.join(authority_count_df, 'LocalAuthorityName').orderBy('RatingValue')
        total_with_percents_df = totals_df.withColumn('RatingPercent', (totals_df.TotalRatings / totals_df.TotalBusinesses) * 100)
        return total_with_percents_df




