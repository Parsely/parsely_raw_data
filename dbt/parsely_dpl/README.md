### Redshift dbt models for Parse.ly DPL

*to do: insert final model image: https://graph.sinterdata.com *

---
## Parse.ly DPL DBT
A "data-build tool" for your Parse.ly data that automates SQL table creation for your data pipeline. It creates queryable tables for things like pageviews, x, and y, and handles the incremental loading of new data from S3 to your SQL tables. By handling this setup work for you, the data-build tool reduces configuration time and lets you get started writing your own custom queries more quickly.

## Schemas/models
pageviews
videoviews
campaigns
content
custom events
sessions
users

## How to get started
- Install DBT
```pip install -r requirements.txt```
- Edit the following files:
-- ~/.dbt/profiles.yml (Input Redshift cluster and database information)
-- parsely_raw_data/dbt/parsely_dpl/dbt_project.yml (Update all variables marked as configurable; details in document)
-- parsely_raw_data/dbt/parsely_dpl/run_parsely_dpl.sh (Details in document)
-- Run historical_script.py (details in document)
-- Set up incremental_script.py to run on an automated schedule


---
- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://dbt.readme.io/docs/installation)
- Join the [chat](http://ac-slackin.herokuapp.com/) on Slack for live questions and support.

---
