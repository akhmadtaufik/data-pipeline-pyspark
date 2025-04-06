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
    memory allocation, network timeout, and PySpark Arrow optimization.
    It also sets the log level to 'ERROR' to minimize log output.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    spark = (
        SparkSession.builder.appName("Data Pipeline")  # type: ignore
        .config("spark.hadoop.home.dir", HADOOP_HOME)
        .config("spark.sql.debug.maxToStringFields", 100)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")  # type: ignore

    return spark
