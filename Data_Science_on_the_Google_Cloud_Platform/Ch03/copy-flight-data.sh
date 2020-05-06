#!/bin/bash

files="
201902.csv
"

counter=0
base_url="gs://elite-caster-125113/flights/raw"

for f in $files
do
   gsutil cp ${base_url}/$f flights.csv-$counter

   # Increment the counter in case of multiple files
   ((counter++))
done
