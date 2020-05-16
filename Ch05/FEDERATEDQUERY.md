## Federated query

Below demonstrates how to use federated query with Google Storage using Bigquery.

### 1. Prepare jq if not installed.

On *fedora 30*  
```
sudo dnf install jq
```

### 2. Export current schema from bigquery.

I will utilize current schema set.

```
bq show --format=prettyjson flights.simevents | jq '.schema.fields' &> simevents.json
```

### 3. Create external table

Some notes for the csv file.

* Columns in csv file MUST match the columns in bigquery.
* Columns MUST be in order of bigquery columns.

```
bq show flights.simevents
```

If everything is clear, create schema.

```
bq mk -- external_table_definition=./simevents.json@CSV=gs://BUCKET/SOMEDIRECTORY/SOMEFILE.csv  flights.fedtzcorr
```

### 4. Run query using standard schema.

```
SELECT
  ORIGIN,
  AVG(DEP_DELAY) as arr_delay,
  AVG(ARR_DELAY) as dep_delay,
FROM
  flights.fedtzcorr   
```

