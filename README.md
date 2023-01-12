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
* Dividend type filter: specify as a filter the type of dividends. In direkavkastining only CD (ordinarie utdelningen) is considered; all the extraordinary dividends are marked as SD (special dividends) by the API :white_check_mark:
* Check direktavkastning in Avanza to understand potential divergences (dividends over the year, not over the rolling past 12 months; tested AGNC,ARR and SCM): :white_check_mark:
* Raise exception when calls limit is reached, but continue run the code (reaching the rump-up invalidate the API calls, so used time.sleep(secs) instead): :white_check_mark: 
* Raise exception if stock does not exist, but continue run the code (raised Exception via requests):  :white_check_mark: 
* Add the DoNotify flag: :white_check_mark: 
* Get posted in slack: :white_check_mark: 
* Decide with Ivano which other notifications we want to show in Slack::white_check_mark: 
* Prettify the notifications/UX design: :white_check_mark: 
* Things to think: asyncronicity between prices and dividends (sometimes prices after new dividends release: which price then?);
sometimes in Avanza stock A has closing price t-2 instead of t-1 and in direktavkastining it is used t-2 instead of t-1;
degration of direktakvastinig back in time because I need API calls to look 1 year back wrt the first stock price in the timeseries: (for time being, it is fine as is!) :white_check_mark: 
* Create a storage csv for stock-alerts-do-notify: : :white_check_mark: 
* API secrets should be contained in a separate thingy:
* Automatize the notification: run the job on a schedule, send the notification when threshold is reached: Airflow
* Check if I need to create virtual env 
* Where to store the NotifyServiceEventData.csv when the job will be chroned/run in Airflow (for now in local):


### Main target variable
The main target variable for this repo is direktavkastning, which is described as follows:
"Direktavkastningen förändras i takt med att aktiekursen förändras. Vi visar direktavkastningen på aktieöversikten och räknar på gårdagens stängningskurs. Under ”Utdelningar” ser du hur hög utdelningen är i kronor. Förutom årets utdelning kan man även se fjolårets, och det kan vara intressant att se om bolaget har ökat eller minskat utdelningen jämfört med tidigare år".

* aktien gårdagens stängningskurs;
* utdelningar: årets utdelning (inte 12 månaders rolling window); bara ordinarie utdelning; varken bonusutdelning (i så fall finns det kapitalöverskott) eller inlösensaktier. Ett exempel: SCM (Stellus Capital Investment Corp) som betalt bonusutdelningar år 2022 på 0,02 USD.   

### How to set up Apache-airflow 
Followed official documentation: https://airflow.apache.org/docs/apache-airflow/stable/start.html

In the terminal, follow the steps:

* Create virtual environment in the repo : virtualenv airflow-hypathia;
* Activate virtuaenv cd airflow-hypathia/bin
* Run source activate
* export AIRFLOW_HOME=~/airflow
* pip install apache-airflow
* airflow db init
* airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
* airflow webserver --port 8080


