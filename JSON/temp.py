from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


spark = SparkSession.builder.getOrCreate()


shows = spark.read.json("./data/shows/shows-silicon-valley.json")

#df.show()

#df = spark.read.json("./data/shows/shows-*.json", multiLine=True)


#print(shows.columns)

#array de strings
array_subset = shows.select("name","genres")

#array_subset.printSchema()

array_subset = array_subset.select(
    "name", 
    array_subset.genres[0].alias("dot_and_index")
    )

array_susbset_repeated = array_subset.select(
    "name",
    F.col("dot_and_index"),
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    ).select(
        "name",
        F.array("one","two","three").alias("Some_genres"),
        F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),
    )
"""
array_susbset_repeated.select(
    "name",
    F.size("Some_genres"),
    F.size("Repeated_Genres"),
    
    ).show()
"""

array_susbset_repeated = array_susbset_repeated.select(
    "name",
    F.array_intersect("Some_genres", "Repeated_Genres").alias("Genres")
)

"""
array_susbset_repeated.select(
    "Genres", F.array_position("Genres", "Comedy")
).show(truncate=False)
"""
columns = ["name","language","type"]

shows_map = shows.select(
    *[F.lit(i) for i in columns],
    F.array(*columns).alias("values"),
)

shows_map = shows_map.select(
    F.array(*columns).alias("keys"),
    "values"
    )

shows_map = shows_map.select(
    F.map_from_arrays("keys","values").alias("mapped"),
)

"""
shows_map.printSchema()

shows_map.select(
    F.col("mapped.name"),
    F.col("mapped")["name"],
    shows_map.mapped["name"],

    ).show(truncate=False)
"""

shows_clean = shows.withColumn(
    "episodes", 
    F.col("_embedded.episodes")
                 ).drop("_embedded")


#shows_clean.printSchema()

episodes_name = shows_clean.select("episodes.name")

"""
episodes_name.select(
    F.explode("name")).show(truncate=False)


"""

episodes = (
    shows.
    select(
        "id", 
        F.
        explode("_embedded.episodes").
        alias("episodes")
    )
)

(

    shows.select(
        F.map_from_arrays(
        "_embedded.episodes.id",
        "_embedded.episodes.name",
        ).alias("name_id")

    ).
    select(
        F.posexplode(
        "name_id"
        ).alias("position","id","name")
    )
)

episodes.groupBy("id").agg(
    F.collect_list("episodes")
    )

(

    shows.
    select(
        F.struct(
            "status",
            "weight", 
            F.lit(True).
            alias("has_watched")
        ).
        alias("info")
    ).
    show()

)