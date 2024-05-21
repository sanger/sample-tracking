#!/bin/bash

# Set environment variables
export REPORTING_DB="mlwh_reporting"
export EVENTS_DB="mlwh_events"

# Generate the SQL file
./cat_sequence.py > runner.sql
# Substitute environment variables in the SQL file and execute it
envsubst < runner.sql | mysql mysql -h mlwh-db.internal.sanger.ac.uk -P 3435 -u mlwh_admin -p