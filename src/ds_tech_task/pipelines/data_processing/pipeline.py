from kedro.pipeline import Pipeline, node
from kedro.runner import SequentialRunner
from .nodes import (
    preprocess_customers_data,
    preprocess_interactions_data,
    preprocess_products_data,
    enrich_interactions_data,
    merge_product_interactions,
    pivot_product_interactions,
    clean_interactions_pivoted,
)


def create_pipeline(**kwargs) -> Pipeline:
    pipeline = Pipeline(
        [
            node(
                func=preprocess_customers_data,
                inputs="customers",
                outputs="customers_preprocessed",
                name="preprocess_customers_data",
            ),
            node(
                func=preprocess_interactions_data,
                inputs="interactions",
                outputs="interactions_preprocessed",
                name="preprocess_interactions_data",
            ),
            node(
                func=preprocess_products_data,
                inputs="products",
                outputs="products_preprocessed",
                name="preprocess_products_data",
            ),
            node(
                func=enrich_interactions_data,
                inputs=["customers_preprocessed", "interactions_preprocessed"],
                outputs="interactions_enriched",
                name="enrich_interactions_data",
            ),
            node(
                func=merge_product_interactions,
                inputs=["interactions_enriched", "products_preprocessed"],
                outputs="product_interactions",
                name="merge_product_interactions",
            ),
            node(
                func=pivot_product_interactions,
                inputs="product_interactions",
                outputs="product_interactions_pivoted",
                name="pivot_product_interactions",
            ),
            node(
                func=clean_interactions_pivoted,
                inputs="product_interactions_pivoted",
                outputs="product_interactions_pivoted_csv",
                name="save_processed_interactions_csv",
            ),
            node(
                func=clean_interactions_pivoted,
                inputs="product_interactions_pivoted",
                outputs="product_interactions_pivoted_json",
                name="save_processed_interactions_json",
            ),
            node(
                func=clean_interactions_pivoted,
                inputs="product_interactions_pivoted",
                outputs="product_interactions_pivoted_xlsx",
                name="save_processed_interactions_xlsx",
            ),
        ]
    )
    return pipeline


# runner = SequentialRunner()
#
# # Run the pipeline
# print(runner.run(pipeline, data_catalog))