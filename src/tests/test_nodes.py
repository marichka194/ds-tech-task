import pytest
import pandas as pd
from datetime import datetime

from src.ds_tech_task.pipelines.data_processing.nodes import *


@pytest.fixture
def interactions():
    return pd.DataFrame(
        [
            {"date_start": datetime(2019, 10, 4), "event": "Email", "customer_id": 1},
            {"date_start": datetime(2020, 2, 11), "event": "Call", "customer_id": 4},
        ]
    )


@pytest.fixture
def customers():
    return pd.DataFrame(
        [
            {"customer_id": 1, "occupation": "Jedi", "type": "Red"},
            {"customer_id": 2, "occupation": "Batman", "type": "Orange"},
            {"customer_id": 3, "occupation": "Santa Claus", "type": "Blue"},
            {"customer_id": 4, "occupation": "Truck", "type": "Orange"},
        ]
    )


@pytest.fixture
def products():
    return pd.DataFrame(
        [
            {"date": datetime(2019, 10, 1), "product": "Sand"},
            {"date": datetime(2020, 2, 1), "product": "Sand"},
            {"date": datetime(2021, 2, 1), "product": "Orange"},
        ]
    )


@pytest.fixture
def product_interactions():
    return pd.DataFrame(
        [
            {
                "date_start": datetime(2019, 10, 4),
                "event": "Email",
                "customer_id": 1,
                "occupation": "Jedi",
                "type": "Red",
                "product_of_interaction": "Sand",
            },
            {
                "date_start": datetime(2020, 2, 11),
                "event": "Call",
                "customer_id": 4,
                "occupation": "Truck",
                "type": "Orange",
                "product_of_interaction": "Sand",
            },
            {
                "date_start": datetime(2020, 3, 21),
                "product_of_interaction": "Call",
                "customer_id": 4,
                "occupation": "Truck",
                "type": "Orange",
                "product": "Sand",
            },
        ]
    )


def test_preprocess_customers_data():
    customers_preprocessed = preprocess_customers_data(
        pd.DataFrame([{"Occupation": "Jedi", "Type": "Red"}])
    )
    assert (
        "occupation" in customers_preprocessed.columns
        and "type" in customers_preprocessed.columns
    )


def test_preprocess_interactions_data():
    interactions_preprocessed = preprocess_interactions_data(
        pd.DataFrame([{"customers": 1}])
    )
    assert "customer_id" in interactions_preprocessed.columns


def test_preprocess_products_data():
    products_preprocessed = preprocess_products_data(
        pd.DataFrame([{"product": "Sand"}])
    )
    assert "product_of_interaction" in products_preprocessed.columns


def test_enrich_interactions_data(interactions, customers):
    interactions_enriched = enrich_interactions_data(
        interactions_preprocessed=interactions, customers_preprocessed=customers
    )
    assert interactions_enriched.shape[0] == 2


def test_merge_product_interactions(interactions, products):
    df_merged = merge_product_interactions(
        interactions_enr=interactions, products_prepr=products
    )
    assert df_merged.shape[0] == 2
    assert list(df_merged["product"]) == ["Sand", "Sand"]


def test_pivot_product_interactions(product_interactions):
    df_pivoted = pivot_product_interactions(product_interactions)
    assert "Email" in df_pivoted.columns and "Call" in df_pivoted.columns
    assert "customer_id" in df_pivoted.columns
    assert "product_of_interaction" in df_pivoted.columns


def test_clean_interactions_pivoted(product_interactions):
    df_preprocessed = clean_interactions_pivoted(
        pd.DataFrame([{"Email": 1, "Boat": 2}])
    )
    assert "email" in df_preprocessed.columns and "boat" in df_preprocessed.columns
