from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np

spark = SparkSession.builder.getOrCreate()

logs = spark.read.csv(
    "./data/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV", 
    sep="|", 
    header=True, 
    inferSchema=True, 
    timestampFormat="yyyy-MM-dd")


column_split = (
    np.array_split(
        np.array(logs.columns), len(logs.columns) // 3
    )
)
"""
for x in column_split:
    logs.select(*x).show(truncate=False)
logs.printSchema()
"""


logs = logs.drop("BroadcastLogID","SequenceNO")   

"""
print("BroadcastLogID" in logs.columns)
print("SequenceNO" in logs.columns)

(

    logs.
    select(
        "Duration", 
        F.col("Duration").substr(1,2).cast("int").alias("dur_hours"),
        F.col("Duration").substr(4,2).cast("int").alias("dur_minutes"),
        F.col("Duration").substr(7,2).cast("int").alias("dur_seconds"),
    ).
    distinct().
    show()

)
"""

(

    logs.
    select(
        "Duration",
        (
            F.col("Duration").substr(1,2).cast("int") * 60 * 60 +
            F.col("Duration").substr(4,2).cast("int") * 60 + 
            F.col("Duration").substr(7,2).cast("int")
        ).
        alias("Duration_seconds") 
    ).
    distinct().
    show()

)





"""
logs.printSchema()

logs.select("BroadcastLogID","LogServiceID","LogDate").show()

logs.select(*["BroadcastLogID","LogServiceID","LogDate"]).show()

logs.select([F.col("BroadcastLogID"),F.col("LogServiceID")]).show()

"""

