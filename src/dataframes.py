from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType


def get_business_data(spark_session):
    """
        Read data from a JSON file 'yelp_academic_dataset_business.json' and return it as a DataFrame.

        This function reads the data from a JSON file 'yelp_academic_dataset_business.json' and creates a DataFrame
        with the specified schema.

        Parameters:
        spark_session (pyspark.sql.SparkSession): The SparkSession object used to create the DataFrame.

        Returns:
        pyspark.sql.DataFrame: A DataFrame containing the data with the specified schema.
    """

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

    return business_df


def get_review_data(spark_session):
    """
            Read data from a JSON file 'yelp_academic_dataset_review.json' and return it as a DataFrame.

            This function reads the data from a JSON file 'yelp_academic_dataset_review.json' and creates a DataFrame
            with the specified schema.

            Parameters:
            spark_session (pyspark.sql.SparkSession): The SparkSession object used to create the DataFrame.

            Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data with the specified schema.
    """

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

    return review_df


def get_user_data(spark_session):
    """
            Read data from a JSON file 'yelp_academic_dataset_user.json' and return it as a DataFrame.

            This function reads the data from a JSON file 'yelp_academic_dataset_user.json' and creates a DataFrame
            with the specified schema.

            Parameters:
            spark_session (pyspark.sql.SparkSession): The SparkSession object used to create the DataFrame.

            Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data with the specified schema.
    """

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

    return user_df


def get_checkin_data(spark_session):
    """
            Read data from a JSON file 'yelp_academic_dataset_checkin.json' and return it as a DataFrame.

            This function reads the data from a JSON file 'yelp_academic_dataset_checkin.json' and creates a DataFrame
            with the specified schema.

            Parameters:
            spark_session (pyspark.sql.SparkSession): The SparkSession object used to create the DataFrame.

            Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data with the specified schema.
    """

    checkin_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True)  # an array of comma separated timestamps
    ])
    checkin_df = spark_session.read.json("data/yelp_academic_dataset_checkin.json", schema=checkin_schema)
    # checkin_df.show()

    return checkin_df


def get_tip_data(spark_session):
    """
            Read data from a JSON file 'yelp_academic_dataset_tip.json' and return it as a DataFrame.

            This function reads the data from a JSON file 'yelp_academic_dataset_tip.json' and creates a DataFrame
            with the specified schema.

            Parameters:
            spark_session (pyspark.sql.SparkSession): The SparkSession object used to create the DataFrame.

            Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data with the specified schema.
    """

    tip_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),  # this is actually TimestampType
        StructField("compliment_count", IntegerType(), True),
    ])
    tip_df = spark_session.read.json("data/yelp_academic_dataset_tip.json", schema=tip_schema)
    # tip_df.show()

    return tip_df
