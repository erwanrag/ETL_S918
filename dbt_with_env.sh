#!/bin/bash
set -a
source /data/prefect/.env
set +a
exec dbt "$@"