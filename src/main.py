import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

import column_names
from dataframes import get_data

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("Spark Project")
                 .config(conf=SparkConf())
                 .getOrCreate())


def describe_data(df):
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
    numeric_columns = [field.name for field in df.schema.fields if
                       isinstance(field.dataType, (IntegerType, FloatType))]

    print("Statistic about numeric columns")
    df.select(numeric_columns).summary().show()


business_df, review_df, user_df, checkin_df, tip_df = get_data(spark_session)

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
