# Introduction 
The project aims to establish a data processing system to assist Bitcoin mining, with the workflow including:

1. Continuously obtaining real-time BTC price indicators and mining metrics (such as network difficulty and network hashrate) from external third-party APIs.

2. After acquiring and cleaning the data, storing the indicators in a local database.

3. Generating a flattened data table with a 5-minute granularity, containing the following indicators: BTC/USD price, network difficulty, and network hashrate.

# Deploy
> Ensure you have installed Docker environment. Because this project need docker compose to start!

First you need to clone this project to your local, and follow these instructions step by step:
1. create `.env` file and add environment variables, including:
- AIRFLOW_UID
- AIRFLOW_PROJ_DIR
- _AIRFLOW_WWW_USER_USERNAME
- _AIRFLOW_WWW_USER_PASSWORD
- SECRET_KEY
- ALLOWED_DESERIALIZATION_CLASSES
2. run `docker compose up -d` 
3. open the `localhost:8080` and trigger the mining task

# Contributor

Author - [lyfer233](https://github.com/lyfer233)