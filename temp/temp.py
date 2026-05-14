from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ale").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

results = (
    spark.
    read.
    text("./data/gutenberg_books/*.txt").
    select(
        F.split(
            "value", 
            pattern=" "
        ).
        alias("line")
    ).
    select(
        F.explode(F.col("line")).
        alias("word")
    ).
    select(
        F.lower("word").
        alias("word_lower")
    ).
    select(
        F.regexp_extract(
            "word_lower", 
            "[a-z]+", 
            0
        ).alias("word")
    ).
    filter(F.col("word") != "").
    groupby(F.col("word")).
    count().
    orderBy("count", ascending=False)
)

results.coalesce(1).write.csv("simple_count.csv", mode="overwrite")
