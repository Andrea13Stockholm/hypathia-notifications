# hypathia-notifications :snowflake: :rainbow: :earth_americas:

This repository uses polygon.io API to get stocks prices and dividends over time to
calculate the dividend price ratio and sent alert based system.

### Repo's features
* Separate the functions from the main .ipynb:  :white_check_mark:
* API secrets should be contained in a separate thingy
* Raise exception if stock does not exist, but continue run the code
* Raise exception when calls limit is reached,  but continue run the code
* Get dividends process and match with prices
* Calculate the dividend price ratio
* Get posted in slack
* Add if lookback period more than 2 years than stop execution (temporary given our account):  :white_check_mark:
* Automatize the notification: run the job on a schedule, send the notification when threshold is reached: Airflow
* Get the required packages outside the main python file:  :white_check_mark:
* Black/np-black formatting:  :white_check_mark:
* List of stocks should be provided separately, with the alert levels:  :white_check_mark:
* Stocks in different markets are not traded on same days: need to create an homogeneous date grid (discrete data points):
* Main code in `get_stock_prices.ipynb` 
* Check if I need to create virtual env 
* Add `.gitignore file`: :white_check_mark:

