import logging
import numpy as np
import pandas as pd


def lower_column_names(df):
    """Converts the df column names to lower case.

    Args:
        df: DataFrame.
    Returns:
        DataFrame with all column names in lower case.
    """

    new_columns_names = [col.lower() for col in df.columns]
    column_mapping = dict(zip(list(df.columns), new_columns_names))
    return df.rename(columns=column_mapping)


def preprocess_customers_data(customers: pd.DataFrame) -> pd.DataFrame:
    """Replaces NaN values with None and converts the column names to lower case.

    Args:
        customers: CRM data about customers.
    Returns:
        DataFrame with customer data and column names in lower case.
    """

    customers[["Occupation", "Type"]] = customers[["Occupation", "Type"]].replace(
        np.nan, "None"
    )

    return lower_column_names(customers)


def preprocess_interactions_data(interactions: pd.DataFrame) -> pd.DataFrame:
    """Renames customers column into customer_id for consistency with other tables.

    Args:
        interactions: Records of interactions with customers.
    Returns:
        DataFrame with the renamed customers column.
    """

    return interactions.rename(columns={"customers": "customer_id"})


def preprocess_products_data(products: pd.DataFrame) -> pd.DataFrame:
    """Renames product column into product_of_interaction.

    Args:
        products: Data about products.
    Returns:
        DataFrame with the renamed product column.
    """

    return products.rename(columns={"product": "product_of_interaction"})


def enrich_interactions_data(
    customers_preprocessed: pd.DataFrame, interactions_preprocessed: pd.DataFrame
) -> pd.DataFrame:
    """Merges interactions data with data about customers.

    Args:
        customers_preprocessed: Preprocessed customers data coming from CRM.
        interactions_preprocessed: Preprocessed data about the interactions with customers.
    Returns:
        DataFrame with the interactions data enriched with customers info.
    """

    interactions_enriched = pd.merge(
        interactions_preprocessed, customers_preprocessed, on="customer_id", how="left"
    )
    return interactions_enriched


def merge_product_interactions(
    interactions_enr: pd.DataFrame, products_prepr: pd.DataFrame
) -> pd.DataFrame:
    """Merges enriched interactions data with products data based on the time of interaction.

    Args:
        interactions_enr: Data about interactions with customers, preprocessed and enriched with customers info.
        products_prepr: Preprocessed products data.
    Returns:
        DataFrame with the interactions data merged with products data.
    """

    interactions_enr["periodic_index"] = interactions_enr["date_start"].dt.to_period("M")
    products_prepr["periodic_index"] = products_prepr["date"].dt.to_period("M")

    product_interactions = pd.merge(
        interactions_enr, products_prepr, how="left", on="periodic_index"
    )
    product_interactions.drop(columns=["periodic_index", "date"], inplace=True)
    return product_interactions


def pivot_product_interactions(product_interactions: pd.DataFrame) -> pd.DataFrame:
    """Pivots prepared interactions data to count number of interactions for each event type.

    Args:
        product_interactions: Data about interactions with customers and products info.
    Returns:
        DataFrame with the number of interactions for each event type.
    """

    df_pivoted = pd.pivot_table(
        product_interactions,
        values="date_start",
        index=["customer_id", "occupation", "type", "product_of_interaction"],
        columns=["event"],
        aggfunc="count",
    )
    df_pivoted.reset_index(inplace=True)

    return df_pivoted


def clean_interactions_pivoted(
    product_interactions_pivoted: pd.DataFrame,
) -> pd.DataFrame:
    """Converts column names to lower case for consistency

    Args:
        product_interactions_pivoted: Interactions data with number of interactions per event type.
    Returns:
        DataFrame with all column names in lower case.
    """

    return lower_column_names(product_interactions_pivoted)
