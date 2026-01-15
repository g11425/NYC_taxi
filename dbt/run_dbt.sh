set -e

if [ $# -lt 1 ]; then
  echo "error missing arguments. please input data_period"
  exit 1
fi

export DATA_PERIOD=$1


eval $(python export_vars_credentials.py)

echo $DBT_REDSHIFT_PASSWORD

dbt run --profiles-dir . --project-dir .

dbt clean 

