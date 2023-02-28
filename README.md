# DS Tech Exercise

## Overview

This project builds a pipeline for cleaning and processing the given CRM data and gives a possibility to save the output
data in CSV, JSON, or Excel format.

## Install dependencies

To install the dependencies, run:

```
pip install -r src/requirements.txt
```

## Run Kedro pipeline

Code of the node-methods and code describing the pipeline can be found in `src/ds_tech_task/pipelines/data_processing/` folder.


To run the pipeline and get output in all the three available formats, run it with:

```
kedro run
```

To run the pipeline and get output in a chosen format, use <i>--to-nodes</i> parameter:

```
kedro run --to-nodes=save_processed_interactions_csv     # for CSV file
kedro run --to-nodes=save_processed_interactions_json    # for JSON file
kedro run --to-nodes=save_processed_interactions_xlsx    # for Excel file
```
The output file will be saved in the `data/04_processed/` folder.

## Test nodes

The unit tests can be found in `src/tests/test_nodes.py`. To run the tests use the following command:

```
kedro test
```
