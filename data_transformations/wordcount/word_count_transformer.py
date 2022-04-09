import logging

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col,split,explode,regexp_replace,trim

def remove_special_characters(column_name:str, df: DataFrame) -> DataFrame :
    return df.select(
        regexp_replace(
        col(column_name),
        "[^A-Za-z0-9']",
        " "
    ).alias(column_name))

def word_count(input_df: DataFrame) -> DataFrame:
    words_df = remove_special_characters("value", input_df).select(
        split(col("value"), " ").alias("words")
    ).select(
        explode(col("words")).alias("word")
    ).filter(trim(col("word")) != '')
    word_counts_df = words_df.groupBy("word").count()
    return word_counts_df

def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    output_df = word_count(input_df)

    logging.info("Writing csv to directory: %s", output_path)
    output_df.coalesce(1).write.csv(output_path, header=True)
