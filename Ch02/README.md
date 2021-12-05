## requirements.txt

Syntax is based on requirements.txt.
https://cloud.google.com/appengine/docs/standard/python3/specifying-dependencies

Flask is chosen for web framework for Python WSGI since it was used in the book,  
Data Engineering on Google Cloud Platform.
Other frameworks available are:

Flask
Django
Pyramid
Bottle
web.py
Tornado

Packages are latest as of Jan 13, 2019


# app.yaml


Yaml file syntax is based on flexible environment of app.yaml.
https://cloud.google.com/appengine/docs/flexible/python/reference/app-yaml

Handler element is based on standard environment of app.yaml.
https://cloud.google.com/appengine/docs/standard/python3/config/appref


# Run program
1. Set environment
```
export GOOGLE_APPLICATION_CREDENTIALS=PATH_TO_JSON_FILE
pip install -r requirements.txt
```
2. Run command
```
usage: ingest_flights.py [-h] --bucket BUCKET [--year YEAR] [--month MONTH] --proj PROJ

ingest flights data from BTS website to GCS

optional arguments:
  -h, --help       show this help message and exit
  --bucket BUCKET  GCS bucket to upload data to.
  --year YEAR      Example: 2015
  --month MONTH    Example: 01
  --proj PROJ      Name of the project

```

To set up scheduler, use app engine.
```
gcloud app deploy --appyaml appyaml
```