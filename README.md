# International Trade Data Pipeline
Data Engineering playground with a broad business motivation to serve SMB with data assets about international trading

## Tech Stack
* Python
* MongoDB (Atlas)

## How to Run
1. Setup infrastructure: register accounts; create credentials; instantiate database etc., and fill into the data_access.cfg file
	* [International Trade Administration](https://api.trade.gov/apps/store/ita/resources)
	* [MongoDB Atlas](https://www.mongodb.com/docs/atlas/getting-started/)
2. Test run in **project root dir**
	* one-time data processing `python data_pipeline/one_time_run.py`

## Primary Folders in Repository
* data_files: datasets not on the cloud will be there
* data_pipeline: where data processing is scheduled and triggered
* data_tasks: the logic and functionality of data processing regardless of date ranges or frequency
