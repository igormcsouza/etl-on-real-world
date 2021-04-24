# Applying ETL on real world problems

This was inspired by a test I did for data engineering at FIEC (CIENTISTA DE 
DADOS - FOCO EM ENGENHARIA DE DADOS). See more about the problem description at 
[about](about/).

## How do I start?

The dependencies are written at [requirements.txt](requirements.txt), start a
virtual environment and install them following the script bellow.

```bash
root$ virtualenv .venv
root$ pip install -r requirements.txt
```

### Antaq problem

Start the application by running this python script (bash command bellow)
[main.py](scripts/antaq-etl/main.py), data will be downloaded automatically.

```bash
root$ python scripts/antaq-etl/main.py
```

> _Note_: The folder [data](data/) is on the root, so running commands outside
> root may cause problems to find files.

## Using apache airflow

Airflow will be running using Docker. This is a nice tool to run Cron Jobs, 
perfect to fetch data ocasionally as we want in this situation.
