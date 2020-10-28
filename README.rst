.. image:: https://travis-ci.org/Parsely/parsely_raw_data.svg?branch=master
    :target: https://travis-ci.org/Parsely/parsely_raw_data

Parse.ly Raw Data
=================

This repository contains Python example code for working with raw data delivered
by Parse.ly's fully-managed Data Pipeline product, at http://parse.ly/data-pipeline.

This Python repository is a suite of tools, mostly usable from the command-line,
which make it easy to evaluate and integrate the Parse.ly raw data.

Customers can use this repository to:

* gain batch and streaming access to the raw data that Parse.ly
  collects from their sites; streaming access is provided via Amazon Kinesis Streams,
  and batch access via Amazon S3

* generate schemas and DDL for common data warehousing tools, such as Redshift,
  BigQuery, and Apache Spark

* create data samples that can be evaluated using in-memory analyst tools such
  as Excel or R Studio (xlsx/csv samples)

To make use of Parse.ly raw data, you must be a customer of Parse.ly's Data Pipeline
product. To gain access for your Parse.ly account, please contact Parse.ly directly
at http://help.parsely.com.

You can download this repository by cloning it from Github, e.g.

::

    $ git clone https://github.com/Parsely/parsely_raw_data.git

Or, you can install it into an environment with `pip`, e.g.

::

    $ pip install parsely_raw_data

The files in this module are named for the services they interface with. You can simply
run modules to use command-line tools provided, or import the modules to script
them yourselves using your own Python scripts.

Module and CLI Guide
~~~~~~~~~~~~~~~~~~~~

If you have the project installed with `pip`, you can use the following console scripts anywhere:

* ``parsely_redshift`` = ``parsely_raw_data.redshift``
* ``parsely_bigquery`` = ``parsely_raw_data.bigquery``
* ``parsely_s3`` = ``parsely_raw_data.s3``
* ``parsely_stream`` = ``parsely_raw_data.stream``
* ``parsely_schema`` = ``parsely_raw_data.docgen``

Alternately, you can clone the repo, and run each module from within the repo directory, like this:

``cd <path_to_parsely_raw_data_repo_directory>``

* ``python -m parsely_raw_data.samples``: Generate data samples in CSV and XLSX format
* ``python -m parsely_raw_data.s3``: Fetch archived event data from Parse.ly S3 Bucket
* ``python -m parsely_raw_data.stream``: Consume a Parse.ly Kinesis Stream of real-time event data
* ``python -m parsely_raw_data.schema``: Inspect schemas for Redshift, BigQuery, and Spark
* ``python -m parsely_raw_data.redshift``: Create an Amazon Redshift table for events and load data
* ``python -m parsely_raw_data.bigquery``: Create a Google BigQuery table for events and load data

Creating a New Version
----------------------

These are the steps that should be followed when releasing a new version of this library

* Increment the version number in ``__init__.py`` according to semantic versioning rules
* ``git commit -m 'increment version'``
* ``git tag x.x.x`` where ``x.x.x`` is the new version number
* ``git push origin master --tags``
* Create a new release for the new tag in github, noting any relevant changes
* Push to PyPI with ``python setup.py sdist upload``

Using the Parse.ly DBT Star Schema in Redshift
----------------------------------------------
* ``python -m parsely_redshift_etl``: A schedule-able command to complete the full ETL into the Redshift star schema using DBT.