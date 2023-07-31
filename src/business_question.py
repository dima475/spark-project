import pyspark.sql.functions as f
from pyspark.sql import Window

import column_names as c

def average_business_rating_in_each_city(business_df):
    """
        Calculates the average business rating for each city based on the input DataFrame.

        Parameters:
            business_df (DataFrame): A PySpark DataFrame containing business data.

        Returns:
            DataFrame: A new PySpark DataFrame with two columns:
                       'City' (str) - The name of the city.
                       'Average number of stars' (float) - The average rating of businesses in that city.
    """

    average_business_rating_df = (business_df.groupBy(f.col(c.city).alias("City"))
                                             .agg(f.avg(c.stars).alias("Average number of stars"))
                                             .orderBy(c.city))
    return average_business_rating_df


def count_of_positive_reviews_for_each_date(review_df):
    """
        Calculates the count of positive reviews for each date based on the input DataFrame.

        Parameters:
            review_df (DataFrame): A PySpark DataFrame containing review data.

        Returns:
            DataFrame: A new PySpark DataFrame with two columns:
                       'Date' (str) - The date of the reviews in the 'y-M-d' format (e.g., '2023-07-28').
                       'Number of positive reviews' (int) - The count of positive reviews with stars greater than
                                                            or equal to 4.8 for each date.
    """

    starts_for_positive_review = 4.8
    positive_reviews_for_each_date_df = (review_df.filter(f.col(c.stars) >= starts_for_positive_review)
                                                  .groupBy(f.date_format(f.col(c.date), "y-M-d").alias("Date"))
                                                  .agg(f.count("*").alias("Number of positive reviews"))
                                                  .orderBy("Number of positive reviews", ascending=False))
    return positive_reviews_for_each_date_df

def top_5_business_in_each_city(business_df):
    """
        Retrieves the top 5 businesses in each city based on their review counts from the input DataFrame.

        Parameters:
            business_df (DataFrame): A PySpark DataFrame containing business data.

        Returns:
            DataFrame: A new PySpark DataFrame with 5 columns:
                       - 'business_id' (str) - The unique identifier for the business.
                       - 'name' (str) - The name of the business.
                       - 'city' (str) - The city where the business is located.
                       - 'review_count' (int) - The number of reviews for the business.
                       - 'Rank by city' (int) - The rank of the business within its city based on review counts.
    """

    window = Window.partitionBy(c.city).orderBy(f.col(c.city).desc(), f.col(c.review_count).desc())
    top_5_by_city_df = (business_df.select(c.business_id, c.name, c.city, c.review_count)
                                   .withColumn("Rank by city", f.dense_rank().over(window))
                                   .filter(f.col("Rank by city") <= 5))
    return top_5_by_city_df

def number_of_users_created_by_year(user_df):
    """
        Calculates the number of users created by each year based on the input DataFrame.

        Parameters:
            user_df (DataFrame): A PySpark DataFrame containing user data.
        Returns:
            DataFrame: A new PySpark DataFrame with two columns:
                       'Year' (str) - The year in which users were created (e.g., '2020').
                       'Number of users created' (int) - The count of users created in this year.
    """

    number_of_users_df = (user_df.groupBy(f.date_format(f.col(c.yelping_since), "y").alias("Year"))
                                 .agg(f.count("*").alias("Number of users created"))
                                 .orderBy("Number of users created", ascending=False))
    return number_of_users_df


def top_business_in_each_city_by_category(business_df, category):
    """
        Retrieves the top businesses in each city that belong to a specific category based on their star ratings.

        Parameters:
            business_df (DataFrame): A PySpark DataFrame containing business data.
            category (str): The category name for which the top businesses should be retrieved.

        Returns:
            DataFrame: A new PySpark DataFrame with 6 columns:
                       - 'business_id' (str) - The unique identifier for the business.
                       - 'name' (str) - The name of the business.
                       - 'city' (str) - The city where the business is located.
                       - 'categories' (str) - A string of comma separated categories to which the business belongs.
                       - 'stars' (float) - The rating of the business.
                       - 'Rank in city by category' (int) - The rank of the business within its city based on star ratings.
    """

    window = Window.partitionBy(c.city).orderBy(f.col(c.city).desc(), f.col(c.stars).desc())
    top_business_in_each_city_by_category_df = (business_df.filter(f.array_contains(f.split(f.col("categories"), ", "), category))
                                                           .select(c.business_id, c.name, c.city, c.categories, c.stars)
                                                           .withColumn("Rank in city by category", f.dense_rank().over(window)))
    return top_business_in_each_city_by_category_df


def negative_reviews_for_business(review_df, business_df):
    """
        Retrieves negative reviews (equal to or below a certain star rating) for each business based on the input DataFrames.

        Parameters:
            review_df (DataFrame): A PySpark DataFrame containing review data.

            business_df (DataFrame): A PySpark DataFrame containing business data.

        Returns:
            DataFrame: A new PySpark DataFrame with following columns:
                       - 'business_id' (str) - The unique identifier for the business.
                       - 'stars' (float) - The rating of the review.
                       - 'name' (str) - The name of the business.
                       - Other columns from the 'Business' DataFrame.
                       - Other columns from the 'Review' DataFrame.
    """

    negative_review_stars = 2
    negative_reviews_for_business_df = (review_df.filter(f.col(c.stars) <= negative_review_stars).withColumnRenamed(c.stars, "stars_review")
                                                 .join(business_df, on=c.business_id)
                                                 .orderBy(f.col(c.name).desc(),  f.col("stars_review").desc()))
    return negative_reviews_for_business_df
