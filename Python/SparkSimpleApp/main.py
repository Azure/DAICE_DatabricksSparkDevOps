import sys

from pyspark.sql import SparkSession
from sparksimpleapp import rdd_csv_col_count


if __name__ == "__main__":
    
    dataInput = sys.argv[1]
    dataOutput = sys.argv[2]

    spark = (SparkSession.builder
                    .appName("SparkSimpleApp")
                    .getOrCreate()
            )

    rdd = spark.sparkContext.textFile(dataInput)

    rdd1 = rdd_csv_col_count(rdd)

    rdd1.toDF().write.mode('overwrite').csv(dataOutput)
