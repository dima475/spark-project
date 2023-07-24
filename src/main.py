import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("Spark Project")
                 .config(conf=SparkConf())
                 .getOrCreate())


def get_data():
    business_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("stars", FloatType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("is_open", IntegerType(), True),  # only 0 or 1 values
        StructField("attributes", MapType(StringType(), StringType()), True),  # object, business attributes to values
        StructField("categories", StringType(), True),  # an array of comma separated strings
        StructField("hours", MapType(StringType(), StringType()), True)
        # an object of key day to value hours, hours are using a 24hr clock
    ])
    business_df = spark_session.read.json("data/yelp_academic_dataset_business.json", schema=business_schema)
    # business_df.show()

    review_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", FloatType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),  # this is actually TimestampType
    ])
    review_df = spark_session.read.json("data/yelp_academic_dataset_review.json", schema=review_schema)
    # review_df.show()

    user_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("yelping_since", StringType(), True),  # this is actually TimestampType
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("elite", StringType(), True),  # an array of comma separated integers
        StructField("friends", StringType(), True),  # an array of comma separated strings
        StructField("fans", IntegerType(), True),
        StructField("average_stars", FloatType(), True),
        StructField("compliment_hot", IntegerType(), True),
        StructField("compliment_more", IntegerType(), True),
        StructField("compliment_profile", IntegerType(), True),
        StructField("compliment_cute", IntegerType(), True),
        StructField("compliment_list", IntegerType(), True),
        StructField("compliment_note", IntegerType(), True),
        StructField("compliment_plain", IntegerType(), True),
        StructField("compliment_cool", IntegerType(), True),
        StructField("compliment_funny", IntegerType(), True),
        StructField("compliment_writer", IntegerType(), True),
        StructField("compliment_photos", IntegerType(), True)
    ])
    user_df = spark_session.read.json("data/yelp_academic_dataset_user.json", schema=user_schema)
    # user_df.show()

    checkin_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True)  # an array of comma separated timestamps
    ])
    checkin_df = spark_session.read.json("data/yelp_academic_dataset_checkin.json", schema=checkin_schema)
    # checkin_df.show()

    tip_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),  # this is actually TimestampType
        StructField("compliment_count", IntegerType(), True),
    ])
    tip_df = spark_session.read.json("data/yelp_academic_dataset_tip.json", schema=tip_schema)
    # tip_df.show()

    return business_df, review_df, user_df, checkin_df, tip_df


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
                       isinstance(field.dataType, IntegerType) or isinstance(field.dataType, FloatType)]

    print("Statistic about numeric columns")
    df.select(numeric_columns).summary().show()


business_df, review_df, user_df, checkin_df, tip_df = get_data()

# print("Information about 'Business' DataFrame")
# describe_data(business_df)
# describe_numeric(business_df)

# print("Information about 'Review' DataFrame")
# describe_data(review_df)
# describe_numeric(review_df)

print("Information about 'User' DataFrame")
describe_data(user_df)
describe_numeric(user_df)
#
# print("Information about 'Checkin' DataFrame")
# describe_data(checkin_df)
# describe_numeric(checkin_df)
#
# print("Information about 'Tip' DataFrame")
# describe_data(tip_df)
# describe_numeric(tip_df)
