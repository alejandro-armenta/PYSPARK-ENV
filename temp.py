from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

elements = spark.read.csv(
    "./data/elements/Periodic_Table_Of_Elements.csv",
    header=True, 
    inferSchema=True
)


elements.where(F.col("phase") == "liq").groupBy("period").count().show()


elements.createOrReplaceTempView("elements")



spark.sql(
    """
    select 
    period, 
    count(*) as count
    from elements 
    where phase == 'liq' 
    group by period
    """
    
).show()