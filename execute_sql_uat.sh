#!/bin/bash

# Set environment variables
export REPORTING_DB="mlwhd_reporting"
export EVENTS_DB="mlwhd_mlwh_events_proddata"


# Generate the SQL file
./cat_sequence.py > runner.sql
# Substitute environment variables in the SQL file and execute it
envsubst < runner.sql | mysql mysql -h mlwhd-db.internal.sanger.ac.uk -P 3436 -u mlwhd_admin -p
