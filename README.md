Project: Pedestrian Analysis
=========

## Description
This repository contains the artefacts requested as part of the application process for a certain data role

## Table of contents
<!--ts-->
   * [High Level Architecture](#high-level-architecture)
   * [Artefacts](#artefacts)
      * [Glue Jupyter Notebooks](#glue-jupyter-notebooks)
      * [Glue ETL Scripts](#glue-etl-scripts)
      * [Parquet Tables in S3](#parquet-tables-in-s3)
      * [Glue Catalog Tables](#glue-catalog-tables)
      * [Testing in DBT Athena](#testing-in-dbt-athena)
   * [IAC - CDK](#iac---cdk)
   * [CICD - GitHub Actions](#cicd---github-actions)
   * [Tests & QA Issues Encountered](#tests--qa-issues-encountered)
   * [Data Model](#data-model)
<!--te-->

High Level Achitecture
============
A user can interact with this product in two ways
- Viewing the notebook reports found in the /glue_notebooks directory
- Pushing a change to the repository will initiate a Github action that deploys to CDK, runs all the glue jobs and runs the DBT tests

![alt text](/images/pa-hl-architecture.png)

Glue Jupyter Notebooks
============
The Jupyter Notebooks are generated using Glue servers - https://docs.aws.amazon.com/glue/latest/dg/console-notebooks.html


Glue ETL Scripts
============

The glue scripts are located in ./glue_job_scripts and are loaded in Glue with CDK
The glue scripts use spark and python to read data from either the glue catalog/s3 or the City of Melbourne API
- This data gets loaded into another glue table which can be queried using Athena
- The GitHub Action associated with this repo runs DBT tests using Athena

Parquet Tables in S3
============
Assets are stored in S3 in the bucket "pedestrian-analysis-working-bucket"
- Data is loaded into here using the Glue Jobs or Notebooks

Glue Catalog Tables
============
Assets are managed in the Glue/Hive Metadata Catalog
- This catalog makes access to this data via Glue, Athena and other platforms significantly more easy

Testing in DBT Athena
============
DBT is used for easily organising, reusing and running tests
- The DBT tests will automatically be run when changes are pushed to this repository
- Reusable/Generic Tests are located in pa_dbt/models/schema.yml
- Specific Tests are located in pa_dbt/tests/


IAC - CDK
============
CDK is used to deploy assets to AWS and allows us to do in a programmatic way with Python
The specification can be found in ./pedestrian_analysis/pedestrian_analysis_stack.py
- The CDK deploys IAM roles, Glue Databases, some s3 files and glue jobs

CICD
============
A GitHub Action is triggered after every push to the main branch of this repository
- The workflow is defined in .github/workflows/deploy-cdk.yml
- The workflow installs all of the packages/dependencies, deploys to CDK, runs all of the glue jobs and all the DBT tests

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