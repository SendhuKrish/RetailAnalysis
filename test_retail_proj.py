import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

def test_read_customers(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435

def test_read_orders(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

def test_read_appconfig():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.transformation()
def test_count_order_status(spark, expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    print(actual_results.collect())
    assert actual_results.collect() == expected_results.collect()

@pytest.mark.latest()
def test_check_closed_count(spark):
    order_df = read_orders(spark, "LOCAL")
    closed_count = filter_orders_generic(order_df, "CLOSED").count()
    assert closed_count == 7556

@pytest.mark.latest()
def test_check_pendingpayment_count(spark):
    order_df = read_orders(spark, "LOCAL")
    closed_count = filter_orders_generic(order_df, "PENDING_PAYMENT").count()
    assert closed_count == 15030

@pytest.mark.latest()
def test_check_complete_count(spark):
    order_df = read_orders(spark, "LOCAL")
    closed_count = filter_orders_generic(order_df, "COMPLETE").count()
    assert closed_count == 22900

@pytest.mark.parametrize(
        "status, count",
        [("CLOSED", 7556),
         ("PENDING_PAYMENT", 15030),
         ("COMPLETE", 22900),
         ]
)

@pytest.mark.generic
def test_check_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, status).count()
    assert filtered_count == count