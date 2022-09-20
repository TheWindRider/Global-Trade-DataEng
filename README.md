# International Trade Data Pipeline
Data Engineering playground with a broad business motivation to serve SMB with data assets about international trading

## Tech Stack
* Python
* MongoDB (Atlas)
* Airflow

## How to Run
1. Setup infrastructure: register accounts; create credentials; instantiate database etc., and fill into the data_access.cfg file
	* [International Trade Administration](https://api.trade.gov/apps/store/ita/resources)
	* [Commodities API](https://commodities-api.com)
	* [MongoDB Atlas](https://www.mongodb.com/docs/atlas/getting-started/)
	* Airflow
2. More than one way to run data processing
	* one-time test run **in project root dir** `python data_pipeline/one_time_run.py`
	* regular data processing via Airflow: [daily_dag, weekly_dag, monthly_dag, etc.]

## Primary Folders in Repository
* data_files: datasets not on the cloud will be there
* data_pipeline: where data processing is scheduled and triggered
* data_tasks: the logic and functionality of data processing regardless of date ranges or frequency
