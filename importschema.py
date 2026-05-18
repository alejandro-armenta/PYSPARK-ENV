from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pprint


spark = SparkSession.builder.getOrCreate()


links = T.StructType(
    [
        T.StructField(
            "self", T.StructType(
                [
                    T.StructField(
                        "href", T.StringType()
                    )
                ]
            )
        )
    ]
)



image = T.StructType(
    [
        T.StructField("medium", T.StringType()),
        T.StructField("original", T.StringType()),
    ]
)


episodes = T.StructType(
    [
        T.StructField(
            "_links", 
            links
        ),

        T.StructField("airdate", T.DateType()),

        T.StructField("airstamp", T.TimestampType()),

        T.StructField("airtime", T.StringType()),
        
        T.StructField(
            "id", 
            T.StringType()
        ),
        
        T.StructField(
            "image", 
            image
        ),
        
        T.StructField("name", T.StringType()),

        T.StructField("number", T.LongType()),
        T.StructField("runtime", T.LongType()),
        T.StructField("season", T.LongType()),
        
        T.StructField("summary", T.StringType()),
        T.StructField("url", T.StringType()),

    ]
)

embedded = T.StructType(
    [
        T.StructField(
    
            "_embedded",
            
            T.StructType(
                [
                    T.StructField(
                        "episodes", 
                        T.ArrayType(episodes)
                    )
                ]
            )
        )
    ]   
)

shows = spark.read.json("./data/shows/shows-silicon-valley.json",
                        schema=embedded,
                        mode="FAILFAST"
                        )


pprint.pprint(
    shows.
    select(
        F.explode("_embedded.episodes").
        alias("episode")
    ).
    select("episode.airstamp").
    schema.
    jsonValue()
)

import json

print(json.loads(shows.schema.json()))

