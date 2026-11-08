#!/usr/bin/env bash
# Loads the example's sample tables into BigQuery: the entity table (who, as of when, and the label) and two
# feature tables. In real use the feature tables belong to whoever produces the features; this only gives the
# example something to read.
#
#   data/load_tables.sh <project> <dataset> <table_prefix>
#
# The prefix is the one new-usecase used (`<usecase>_` with dashes as underscores). Needs the bq CLI; run it as
# someone who may create tables in the dataset. The runner needs read access (it has it on its own dataset).
set -euo pipefail
project=${1:?project}; dataset=${2:?dataset}; prefix=${3:?table prefix}
here=$(cd "$(dirname "$0")" && pwd)

load() {
  bq --project_id="$project" load --replace --source_format=CSV --skip_leading_rows=1 \
    "$dataset.$prefix$1" "$here/$1.csv" "$2"
}
load entities "customer_id:STRING,snapshot_date:DATE,churned:INT64"
load customer_profile "customer_id:STRING,feature_timestamp:TIMESTAMP,tenure_months:INT64,monthly_charges:FLOAT64,total_charges:FLOAT64,contract_length_months:INT64,num_products:INT64,has_paperless_billing:INT64"
load support_activity "customer_id:STRING,feature_timestamp:TIMESTAMP,num_support_tickets:INT64,avg_monthly_usage_gb:FLOAT64"
echo "train with: pipewright run --env <env> --usecase <usecase> --source-uri bq://$project.$dataset.${prefix}entities"
