# DS Tech Exercise

## Overview

This project builds a pipeline for cleaning and processing the given CRM data and gives a possibility to save the output
data in CSV, JSON, or Excel format.

## Install dependencies

Declare any dependencies in `src/requirements.txt` for `pip` installation and `src/environment.yml` for `conda` installation.

To install them, run:

```
pip install -r src/requirements.txt
```

## Run Kedro pipeline

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
