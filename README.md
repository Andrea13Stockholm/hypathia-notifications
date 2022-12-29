# hypathia-notifications :snowflake: :rainbow: :earth_americas:

This repository uses polygon.io API to get stocks prices and dividends over time to
calculate the dividend price ratio and sent alert based system.

### Repo's features
* Separate the functions from the main .ipynb: :white_check_mark:
* Get dividends process: :white_check_mark:
* Add if lookback period more than 2 years than stop execution (temporary given our account): :white_check_mark:
* Get the required packages outside the main python file:  :white_check_mark:
* Black/np-black formatting:  :white_check_mark:
* List of stocks should be provided separately, with the alert levels:  :white_check_mark:
* Add `.gitignore file`: :white_check_mark:
* Main code in `get_stock_prices.ipynb`: :white_check_mark:
* Add time.sleep to slow down the rump-up when making API calls: :white_check_mark:
* Stocks in different markets are not traded on same days: need to create an homogeneous date grid (discrete data points):  :white_check_mark:
* Get the dividends process to be called upon price process timestamps: :white_check_mark:
* Calculate the dividend price ratio: :white_check_mark:
* Check direktavkastning in Avanza to understand potential divergences:
* API secrets should be contained in a separate thingy:
* Raise exception if stock does not exist, but continue run the code:
* Raise exception when calls limit is reached, but continue run the code:
* Get posted in slack:
* Automatize the notification: run the job on a schedule, send the notification when threshold is reached: Airflow
* Check if I need to create virtual env 

### Main target variable
The main target variable for this repo is direktavkastning, which is described as follows:
"Direktavkastningen förändras i takt med att aktiekursen förändras. Vi visar direktavkastningen på aktieöversikten och räknar på gårdagens stängningskurs. Under ”Utdelningar” ser du hur hög utdelningen är i kronor. Förutom årets utdelning kan man även se fjolårets, och det kan vara intressant att se om bolaget har ökat eller minskat utdelningen jämfört med tidigare år".

