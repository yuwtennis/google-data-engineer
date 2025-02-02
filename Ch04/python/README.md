## python
### Python module version used
```
$ python -V
Python 2.7.15`
```

```
$ pip freeze | egrep "beam|timezone|pytz|python"
apache-beam==2.11.0
pytz==2019.1
timezonefinder==1.5.4
```

### CODES
1. self-df01.py
  Sample code for executing pipeline in DirectRunner mode

2. self-df02.py
  Sample code for executing pipeline in DataFlow mode

  Usage:
  python self-df02.py --input=gs://[DIR]/89598257_T_MASTER_CORD.csv --output=gs://[DIR]/airport.csv --project=[PROJECTID] --job_name=[JOB NAME] --staging_location=gs://[Staging directory] --temp_location=gs://[Temporary directory]

3. self-df0333y
  Sample code for executing pipeline in DirectRunner mode

### How to run

```shell
python3 self-df05.py \
  --bucket elite-caster-125113 \
  --dataset elite-caster-125113:flights2019 \
  --runner=DataflowRunner \
  --region asia-northeast1 \
  --project=elite-caster-125113 \
  --job_name=ch04timecorr \
  --region=asia-northeast1 \
  --save_main_session \
  --staging_location=gs://elite-caster-125113/flights/staging/ \
  --temp_location=gs://elite-caster-125113/flights/temp/ \
  --requirements_file=./requirements.txt \
  --experiment use_unsupported_python_version
```