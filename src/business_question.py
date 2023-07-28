import pyspark.sql.functions as f
from pyspark.sql import Window

import column_names as c

def average_business_rating_in_each_city(business_df):
    average_business_rating_df = (business_df.groupBy(f.col(c.city).alias("City"))
                                             .agg(f.avg(c.stars).alias("Average number of stars"))
                                             .orderBy(c.city))
    return average_business_rating_df


def count_of_positive_reviews_for_each_date(review_df):
    starts_for_positive_review = 4.8
    positive_reviews_for_each_date_df = (review_df.filter(f.col(c.stars) >= starts_for_positive_review)
                                                  .groupBy(f.date_format(f.col(c.date), "y-M-d").alias("Date"))
                                                  .agg(f.count("*").alias("Number of positive reviews"))
                                                  .orderBy("Number of positive reviews", ascending=False))
    return positive_reviews_for_each_date_df

def top_5_business_in_each_city(business_df):
    window = Window.partitionBy(c.city).orderBy(f.col(c.city).desc(), f.col(c.review_count).desc())
    top_5_by_city_df = (business_df.select(c.business_id, c.name, c.city, c.review_count)
                                   .withColumn("Rank by city", f.dense_rank().over(window))
                                   .filter(f.col("Rank by city") <= 5))
    return top_5_by_city_df

def number_of_users_created_by_year(user_df):
    number_of_users_df = (user_df.groupBy(f.date_format(f.col(c.yelping_since), "y").alias("Year"))
                                 .agg(f.count("*").alias("Number of users created"))
                                 .orderBy("Number of users created", ascending=False))
    return number_of_users_df


def top_business_in_each_city_by_category(business_df, category):
    window = Window.partitionBy(c.city).orderBy(f.col(c.city).desc(), f.col(c.stars).desc())
    top_business_in_each_city_by_category_df = (business_df.filter(f.array_contains(f.col(c.categories), category))
                                                           .select(c.business_id, c.name, c.city, c.categories, c.stars)
                                                           .withColumn("Rank in city by category", f.dense_rank().over(window)))
    return top_business_in_each_city_by_category_df


def negative_reviews_for_business(review_df, business_df):
    negative_review_stars = 2
    negative_reviews_for_business_df = (review_df.filter(f.col(c.stars) <= negative_review_stars)
                                                 .join(business_df, on=c.business_id)
                                                 .orderBy(business_df[c.name].desc(), review_df[c.stars].desc()))
    return negative_reviews_for_business_df
