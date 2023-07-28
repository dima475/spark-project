from pyspark.sql.types import IntegerType, FloatType


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
