from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower

spark = SparkSession.builder.appName("ale").getOrCreate()

book = spark.read.text("./data/gutenberg_books/1342-0.txt")

lines = (
    book.
    select(
        split(
            "value", 
            pattern=" "
        ).
        alias("line")
    )
)

words = (

    lines.
    select(
        explode(col("line")).
        alias("word")
    )
)

words_lower = (
    words.
    select(
        lower("word").
        alias("word_lower")
    )
)

