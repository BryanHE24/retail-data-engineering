import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Start Spark once and close it after all tests finish."""

    # Make Spark use the same Python as our active virtual environment.
    os.environ["PYSPARK_PYTHON"] = sys.executable

    # Start local Spark and enable details if a Python worker crashes.
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("retail-unit-tests")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )

    # Give the session to each test that requests "spark".
    yield session

    # Release Spark resources when testing finishes.
    session.stop()