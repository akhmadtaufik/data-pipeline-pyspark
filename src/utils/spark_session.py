import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql

HADOOP_HOME = "C:\\hadoop"

findspark.init()  # type: ignore

os.environ["HADOOP_HOME"] = HADOOP_HOME


def init_spark_session() -> SparkSession:
    """
    Initialize and configure a SparkSession for the data pipeline.

    This function sets up a SparkSession with a specified application name
    and configuration settings, including the Hadoop home directory and
    maximum number of fields to display in debug output. It also sets the
    log level of the Spark context to "ERROR" to minimize log verbosity.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    spark = (
        SparkSession.builder.appName("Data Pipeline")
        .config("spark.hadoop.home.dir", HADOOP_HOME)
        .config("spark.sql.debug.maxToStringFields", 100)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark
