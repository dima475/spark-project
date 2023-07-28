import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession

from dataframes import get_business_data, get_review_data, get_user_data, get_checkin_data, get_tip_data, load_data
from research import describe_data, describe_numeric
from business_question import average_business_rating_in_each_city, count_of_positive_reviews_for_each_date, top_5_business_in_each_city, number_of_users_created_by_year, top_business_in_each_city_by_category, negative_reviews_for_business

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("Spark Project")
                 .config(conf=SparkConf())
                 .getOrCreate())

business_df = get_business_data(spark_session)
review_df = get_review_data(spark_session)
user_df = get_user_data(spark_session)
# checkin_df = get_checkin_data(spark_session)
# tip_df = get_tip_data(spark_session)


# print("Information about 'Business' DataFrame")
# describe_data(business_df)
# describe_numeric(business_df)
#
# print("Information about 'Review' DataFrame")
# describe_data(review_df)
# describe_numeric(review_df)
#
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

# result_one = average_business_rating_in_each_city(business_df)
# result_one.show()
#
# result_two = count_of_positive_reviews_for_each_date(review_df)
# result_two.show()
#
# result_three = top_5_business_in_each_city(business_df)
# result_three.show()
#
# result_four = number_of_users_created_by_year(user_df)
# result_four.show()
#
# result_five = top_business_in_each_city_by_category(business_df, "Food")
# result_five.show()
#
# result_six = negative_reviews_for_business(review_df, business_df)
# result_six.show()
