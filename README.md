Project: Pedestrian Analysis
=========

## Description
This repository contains the artefacts requested as part of the application process for a certain data role

The reports below are the primary artefacts
- [Top 10 Locations By Day](glue_notebooks/report-top-10-locations-per-day-notebook.ipynb)
- [Top 10 Locations By Month](glue_notebooks/report-top-10-locations-by-month-notebook.ipynb)
- [Most Decline Due to Lockdowns](glue_notebooks/report-most-decline-due-to-lockdown-notebook.ipynb)
- [Most Growth Last Year](glue_notebooks/report-most-growth-last-year-notebook.ipynb.ipynb)

## Table of contents
<!--ts-->
   * [High Level Architecture](#high-level-architecture)
   * [Artefacts](#artefacts)
      * [Glue Jupyter Notebooks](#glue-jupyter-notebooks)
      * [Glue ETL Scripts](#glue-etl-scripts)
      * [Parquet Tables in S3](#parquet-tables-in-s3)
      * [Glue Catalog Tables](#glue-catalog-tables)
      * [Testing in DBT Athena](#testing-in-dbt-athena)
   * [CICD - GitHub Actions](#cicd---github-actions)
   * [IAC - CDK](#iac---cdk)
   * [Tests & QA Issues Encountered](#tests--qa-issues-encountered)
   * [Data Model](#data-model)
   * [Source Data](#source-data)
<!--te-->

High Level Achitecture
============

A user can interact with this product in three ways
- Viewing the notebook reports found here - [Glue Notebooks](glue_notebooks/)
- Pushing a change to the repository will initiate a Github action that deploys to CDK, runs all the glue jobs and runs the DBT tests
  - [Example test run](https://github.com/hugh-nguyen/pedestrian-analysis/actions/runs/4847011503/jobs/8636882021)
- Exploration through Athena with SQL compliant interface

![alt text](/images/pa-hl-architecture.png)

Glue Jupyter Notebooks
============

The Jupyter Notebooks are generated using Glue Notebook servers 
- https://docs.aws.amazon.com/glue/latest/dg/console-notebooks.html

You can find the Jupyter Notebooks below
- [Top 10 Locations By Month](glue_notebooks/report-top-10-locations-by-day-notebook.ipynb)
- [Top 10 Locations By Month](glue_notebooks/report-top-10-locations-by-month-notebook.ipynb)
- [Most Decline Due to Lockdowns](glue_notebooks/report-most-decline-due-to-lockdown-notebook.ipynb)
- [Most Growth Last Year](glue_notebooks/report-most-growth-last-year-notebook.ipynb.ipynb)

Glue ETL Scripts
============

The glue scripts are located in [Glue Job Scripts](/glue_job_scripts) and are loaded in Glue with CDK
The glue scripts use spark and python to read data from either the glue catalog/s3 or the City of Melbourne API
- This data gets loaded into another glue table which can be queried using Athena
- The GitHub Action associated with this repo runs DBT tests using Athena
![alt text](/images/glue-jobs.png)

All glue jobs are ran as part of the GitHub Action in order to allow for testing
![alt text](/images/run-glue-jobs.png)

Parquet Tables in S3
============

Assets are stored in S3 in the bucket "pedestrian-analysis-working-bucket"
- Data is loaded into here using the Glue Jobs or Notebooks
![alt text](/images/pa-s3-parquet-1.png)
![alt text](/images/pa-s3-parquet-2.png)

Glue Catalog Tables
============

Assets are managed in the Glue/Hive Metadata Catalog
- This catalog makes access to this data via Glue, Athena and other platforms significantly more easy
![alt text](/images/pa-glue-cat-1.png)
![alt text](/images/pa-glue-cat-2.png)

Testing in DBT Athena
============
DBT is used for easily organising, reusing and running tests
- The DBT tests will automatically be run when changes are pushed to this repository
- Reusable/Generic Tests are located in pa_dbt/models/schema.yml
- Specific Tests are located in [DBT Tests](pa_dbt/tests)
- You can see some of the tests being run here as an example - [Example](https://github.com/hugh-nguyen/pedestrian-analysis/actions/runs/4847229418/jobs/8637297045)

![alt text](/images/pa-dbt-pass.png)

CICD
============

A GitHub Action is triggered after every push to the main branch of this repository
- The workflow is defined in [/.github/workflows/deploy-cdk.yml](/.github/workflows/deploy-cdk.yml)
- The workflow installs all of the packages/dependencies, deploys to CDK, runs all of the glue jobs and all the DBT tests
![alt text](/images/cicd.png)
![alt text](/images/cicd2.png)


IAC - CDK
============

CDK is used to deploy assets to AWS and allows us to do in a programmatic way with Python
The specification can be found in [pedestrian_analysis/pedestrian_analysis_stack.py]/pedestrian_analysis/pedestrian_analysis_stack.py
- The CDK deploys IAM roles, Glue Databases, some s3 files and glue jobs
![alt text](/images/cdk.png)

Tests & QA Issues Encountered
============
You can view some of the tests that have been run previously on the data using DBT here

https://github.com/hugh-nguyen/pedestrian-analysis/actions/runs/4844324305/jobs/8632539622

Issues found are below

- The first case shows us the location_type of sensor_reference_data has to two outlier values "Indoor Blix" and "Outdoor Blix"
- Location name is often null in report_top_10_locations_by_day, and this is because a lot of 
sensor_ids are missing from the reference data
- Direction_1, Direction_2 and installation_date have unexpected null values

![alt text](/images/pa-dbt-1.png)
![alt text](/images/pa-dbt-2.png)

Data Model
============
There are six data assets
- raw.sensor_counts
- raw.sensor_reference_data
- report.location_declines_due_to_lockdown
![alt text](/images/pa-data-model.png)

Source Data
============
https://data.melbourne.vic.gov.au/explore/dataset/pedestrian-counting-system-sensor-locations/information/
https://melbournetestbed.opendatasoft.com/explore/dataset/pedestrian-counting-system-monthly-counts-per-hour/information/