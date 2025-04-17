import os
import findspark
from pyspark.sql import SparkSession

HADOOP_HOME = "C:\\hadoop"

findspark.init()  # type: ignore

os.environ["HADOOP_HOME"] = HADOOP_HOME


def init_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for a data pipeline.

    This function sets up a SparkSession with specific configurations for
    memory allocation, garbage collection, shuffle partitioning, and parallelism.
    It also enables PySpark Arrow for optimized execution. The Spark context
    log level is set to "ERROR" to minimize log output.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    spark = (
        SparkSession.builder.appName("Data Pipeline")  # type: ignore
        .config("spark.hadoop.home.dir", HADOOP_HOME)
        .config("spark.sql.debug.maxToStringFields", 100)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.memory.fraction", "0.5")
        # Optimasi partisi shuffle
        .config("spark.sql.shuffle.partitions", "5")
        # Optimasi GC
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35",
        )
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        # Batasi paralelisme
        .config("spark.default.parallelism", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")  # type: ignore

    return spark
