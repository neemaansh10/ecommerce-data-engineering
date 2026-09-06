import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    PyTest fixture to initialize a lightweight local SparkSession for unit testing.
    """
    session = (
        SparkSession.builder
        .appName("PyTestSparkSession")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
