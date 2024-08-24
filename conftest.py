import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
   sparksession =  get_spark_session("LOCAL")
   yield sparksession
   sparksession.stop()

@pytest.fixture
def expected_results(spark):
   "gives the expected results"
   results_schema = "state string, count int"
   return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(results_schema) \
        .load("data/test results/expected_results.csv")
