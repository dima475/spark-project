import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

import column_names as c
from dataframes import get_business_data, get_review_data, get_user_data, get_checkin_data, get_tip_data

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("Spark Project")
                 .config(conf=SparkConf())
                 .getOrCreate())


def describe_data(df):
    """
        Provides a basic description of the DataFrame.

        This function prints the names of columns, the number of rows, the number of columns,
        and the schema of the DataFrame.

        Parameters:
        df (pyspark.sql.DataFrame): The DataFrame to be described.

        Returns:
        None: This function does not return anything; it prints the information directly.
    """

    print("Names of columns")
    print(df.columns)
    print()

    print("Number of rows")
    print(df.count())
    print()

    print("Number of columns")
    print(len(df.columns))
    print()

    print("Schema of DataFrame")
    df.printSchema()
    print()


def describe_numeric(df):
    """
        Generate descriptive statistics for numeric columns in the DataFrame.

        This function calculates and displays summary statistics for the numeric columns
        (columns with data types 'IntegerType' or 'FloatType') in the DataFrame.

        Parameters:
        df (pyspark.sql.DataFrame): The DataFrame containing numeric columns.

        Returns:
        None: This function does not return anything; it prints the summary statistics directly.
    """

    numeric_columns = [field.name for field in df.schema.fields if
                       isinstance(field.dataType, (IntegerType, FloatType))]

    print("Statistic about numeric columns")
    df.select(numeric_columns).summary().show()


business_df = get_business_data(spark_session)
review_df = get_review_data(spark_session)
user_df = get_user_data(spark_session)
checkin_df = get_checkin_data(spark_session)
tip_df = get_tip_data(spark_session)

# print("Information about 'Business' DataFrame")
# describe_data(business_df)
# describe_numeric(business_df)

# print("Information about 'Review' DataFrame")
# describe_data(review_df)
# describe_numeric(review_df)

# print("Information about 'User' DataFrame")
# describe_data(user_df)
# describe_numeric(user_df)
#
# print("Information about 'Checkin' DataFrame")
# describe_data(checkin_df)
# describe_numeric(checkin_df)
#
# print("Information about 'Tip' DataFrame")
# describe_data(tip_df)
# describe_numeric(tip_df)
