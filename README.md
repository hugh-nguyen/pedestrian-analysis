Project: Pedestrian Analysis
=========

## Description
This repository contains the artefacts requested as part of the application process for a certain data role

Table of contents
=================

## Table of contents
<!--ts-->
   * [Artefacts](#artefacts)
      * [Glue/Spark Scripts](#gluespark-scripts)
      * [Parquet Tables in S3](#parquet-tables-in-s3)
      * [Glue Catalog Tables](#glue-catalog-tables)
      * [Glue Jupyter Notebooks](#glue-jupyter-notebooks)
      * [Testing in DBT Athena](#testing-in-dbt-athena)
   * [IAC - CDK](#iac---cdk)
   * [CICD - GitHub Actions](#cicd---github-actions)
   * [Usage](#usage)
<!--te-->

Glue Scripts
============
The glue scripts use spark and python to read data from either the glue catalog/s3 or the City of Melbourne API
    - This data gets loaded into another glue table which can be queried using Athena
    - The GitHub Action associated with this repo runs DBT tests using Athena

Parquet Tables in S3
============

...

Glue Catalog Tables
============

...

Glue Jupyter Notebooks
============

...

Testing in DBT Athena
============

...

IAC - CDK
============

...

CICD
============

...

Usage
============

Deploy components to AWS

The technical assets are listed below
    - Glue/Spark Jobs
    - Glue Jupyter Notebooks
    - Parquet Tables stored in S3
    - DBT Athena tests

Continuous Deployment is managed using
    - GitHub Actions

Infrastructure as code is implemented using
    - CDK/CloudFormation

