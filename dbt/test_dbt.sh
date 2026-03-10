set -e

if [ $# -lt 1 ]; then
  echo "error missing arguments. please input data_period"
  exit 1
fi

export DATA_PERIOD=$1


eval $(python export_vars_credentials.py)

dbt test --profiles-dir . --project-dir .

# dbt clean 

