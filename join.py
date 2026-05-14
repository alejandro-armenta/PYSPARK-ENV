"""Checkpoint code for the book Data Analysis with Python and PySpark, Chapter 4."""

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

DIRECTORY = "./data/broadcast_logs"
logs = (
    spark.read.csv(
        os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd",
    )
    .drop("BroadcastLogID", "SequenceNO")
    .withColumn(
        "duration_seconds",
        (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
        ),
    )
)

#logs.printSchema()

log_id = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

"""
logs.printSchema()
log_id.printSchema()

log_id.show()

log_id.where(
    F.col("PrimaryFG") == 2
             ).show()
"""

log_id = log_id.where(
    F.col("PrimaryFG") == 1
             )

#print(log_id.count())


logs_and_channels = logs.join(
    log_id, 
    on="LogServiceID",
    how="inner"
          )

"""
ale.printSchema()

ale.select("LogServiceID","LogIdentifierID","PrimaryFG").show()

logs_and_channels = logs.join(
    log_id, 
    on=logs["LogServiceID"] == log_id["LogServiceID"],
    how="inner"
          )

ale.printSchema()

ale.drop(
    log_id["LogServiceID"]
    ).select("LogServiceID").show()


ale = logs.alias("left").join(
    log_id.alias("right"), 
    on=logs["LogServiceID"] == log_id["LogServiceID"],
    how="inner")

ale.printSchema()


ale.drop(
    F.col("right.LogServiceID")
    ).select("LogServiceID").show()
"""


cd_category = (
    spark.read.csv(
        os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
        sep="|",
        header=True,
        inferSchema=True,
    ).
    select(
        "CategoryID",
        "CategoryCD",
        F.col("EnglishDescription").alias("Category_Description")
    )
)

cd_program = (
    spark.read.csv(
        os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
        sep="|",
        header=True,
        inferSchema=True,
    ).
    select(
        "ProgramClassID",
        "ProgramClassCD",
        F.col("EnglishDescription").alias("ProgramClass_Description")
    )
)

full_log = (

    logs_and_channels.
    join(
        cd_category, 
        "CategoryID", 
        how="left"
    ).
    join(
        cd_program, 
        "ProgramClassID", 
        how="left"
    )

)


#22 program clases 
(
    full_log.
    
    groupBy(
        "ProgramClassCD",
        "ProgramClass_Description"
    ).
    
    agg(
        
        F.
        sum("duration_seconds").
        alias("duration_total")

    ).

    orderBy("duration_total", ascending=False)
)

full_log.printSchema()



(
    full_log.
    select(
        F.col("ProgramClassCD")
    ).show()
)

full_log = (
    full_log.
    withColumn("ale",
        F.when(
            F.
            trim(F.col("ProgramClassCD")).
            isin(
                ["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]
            ),
            F.col("duration_seconds")
        ).otherwise(0)
    )
)



answer = (
    full_log.
    groupBy("LogIdentifierID").
    agg(
        F.sum(F.col("ale")).alias("duration_comercial"), 
        F.sum(F.col("duration_seconds")).alias("duration_total"),
    ).
    withColumn(
        "commercial_ratio",
        F.col("duration_comercial") / F.col("duration_total")
    )
)


answer.printSchema()


(
    answer.
    orderBy(
        "commercial_ratio",
        ascending=False
    ).
    show(1000,False)
)


answer_no_null = (
    answer.
    dropna(subset=["commercial_ratio"]).
    orderBy(
        "commercial_ratio",
        ascending=False
    )
)

answer_no_null = (
    answer.fillna(0).
    orderBy(
        "commercial_ratio",
        ascending=False
    )
)

