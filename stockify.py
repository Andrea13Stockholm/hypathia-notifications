'''
Set of function to import financial data from API 
'''
import pandas as pd 


def get_list_stocks_tickers(text_file_name: str,
                            only_stock_tickers:bool) -> list:
    
    with open(text_file_name, "r") as f:
        '''
        Read text file with lists of stocks tickers and return a
        dataframe
        ''' 
        head = [next(f).strip()][0].split(",")
        df = pd.DataFrame(columns=head)
        lines = f.readlines()
        n_line = -1
        for line in lines:
            s = [line.strip()][0].split(",")
            n_line +=1 
            for n_col in range(len(s)):
                df.loc[n_line,head[n_col]]=s[n_col]
                
        if only_stock_tickers == True:
            stock_tickers = list(df.iloc[:,1]) 
            return stock_tickers
        else:
            return df
        
        
def check_max_lookback_years(max_lookback_years: float):
    '''
    Check to avoid inserting over limit lookback period. 
    If more than 2 years are inserted, the function forces
    the limit to 2 years.
    '''
    
    if max_lookback_years > 2:
        max_lookback_years = 2
        
    return max_lookback_years


def is_numb_in_range(numb,lower_bound,upper_bound):
    '''
    Check if number is between to given numbers,
    extremes included.
    ''' 
    
    if lower_bound > upper_bound:
        exit
    else:
        if numb in range(lower_bound,upper_bound+1):
            return True
        else:
            return False


def get_daily_time_buckets(max_lookback_years:float,
                           current_is_timestamp:bool,
                           frequency:str,
                           include_weekend:bool 
                           ) -> pd.Series: 
    
    ''' 
    Get a range of discrete time points, given a current
    time as final data point. Frequency: daily or monthly.
    '''
    
    max_lookback_years = check_max_lookback_years(max_lookback_years)
    
    import pandas as pd 
    import datetime as dt 
    
    if current_is_timestamp:
        current = dt.datetime.now()
    else:
        current = dt.datetime.now().date()
        
    if frequency == 'daily' and include_weekend == True:
        freq = 'D'
    
    elif frequency == 'daily' and include_weekend == False: 
        freq = 'B'
    
    elif frequency == 'monthly' and include_weekend == False: 
        freq = 'BM'
        
    elif frequency == 'monthly' and include_weekend == True: 
        freq = 'M'
        
    if frequency =='daily':
        periods = 365 * max_lookback_years
        
    elif frequency =='monthly':
        periods = 12 * max_lookback_years
    
    
    time_buckets = pd.date_range(start=None, 
                                 end=current, 
                                 periods=periods, 
                                 freq=freq,
                                 tz='UTC', 
                                 inclusive='both') 
    return time_buckets 


def get_HTTP_response_status_code_get_call(ticker:str,
                                           API_key:str):
    
    '''
    Provides HTTP status codes on host for GET calls with
    verbose responses for status codes ranges [200,299],
    [300,399], [400,599]. Outside of ranges codes raise a
    warning.
    ''' 
        
    import requests
    import json
    import datetime as dt 
    from datetime import timedelta as td  

    host='https://api.polygon.io'
    
    current_date = dt.datetime.now().date()
    start = current_date - td(days=7)
    
    url = f'{host}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{current_date}?adjusted=true&sort=asc&limit=120&apiKey={API_key}'

    status_code, response_body = requests.get(url).status_code, json.loads(requests.get(url).text)
    response_status = response_body['status']

    if is_numb_in_range(status_code,200,299) and response_status.lower() !='error':
        response_content = response_body['resultsCount']
            
    elif is_numb_in_range(status_code,200,299) and response_status.lower() =='error':
        response_content = response_body['error']
            
    elif is_numb_in_range(status_code,400,599):
        response_content = str(response_body['error'][0:47])

    else:
        response_content=''
        print("HTTP status code, not codified: " + str(status_code))
        
    object_response = [ticker,status_code,response_status,response_content]
    
    if object_response[1]==200 and object_response[2]!='ERROR' and object_response[3]>0:
        forward_API_request = True
    else:
        forward_API_request = False
    
    return object_response,forward_API_request


def get_HTTP_status_codes_by_sequencing_get_calls(API_keys:list,
                                                  stocks_tickers:list,
                                                  time_sleep:float):
        
        '''
        Hackey way to get status codes for all the stocks using API keys
        from n different accounts.
        '''      
        
        import math as m 
        import numpy as np
        import pandas as pd
        import time
        
        count=0
        
        API_keys_used = []
        object_responses=[]
        forward_API_requests=[]
        stocks=[]
        
        for st in stocks_tickers:
                stocks.append(st)
                count +=1
                number_API_keys = len(API_keys)
                dec_part = np.round(m.modf(count/number_API_keys)[0],4)

                if dec_part == np.round(1/number_API_keys,4):
                        pos_key = 0
                elif dec_part== np.round(2/number_API_keys,4):
                        pos_key = 1  
                else:
                        pos_key = 2
        
                object_response,forward_API_request= get_HTTP_response_status_code_get_call(ticker=st,
                                                                                        API_key=API_keys[pos_key])
                
                time.sleep(time_sleep)
                API_keys_used.append(API_keys[pos_key])
                object_responses.append(object_response)
                forward_API_requests.append(forward_API_request)

        HTTP_summary = pd.concat([pd.Series(API_keys_used),
                                  pd.Series(stocks),
                                  pd.Series(object_responses),
                                  pd.Series(forward_API_requests)],axis=1,
                                 keys=['API_key','stock','Object_response','Forward_API_request'])
        return HTTP_summary


def get_stock_list_GET_calls(HTTP_summary:pd.DataFrame):
    
    '''
    Get the list of stocks to get data after
    preliminary check call on the API. 
    '''

    stocks_ticker_GET = []

    for n_rows in range(0,HTTP_summary.shape[0]):
        
        if HTTP_summary.loc[n_rows,'Forward_API_request']==True:
            
            stocks_ticker_GET.append(HTTP_summary.loc[n_rows,'stock'])
            
        else:
            
            print(HTTP_summary.loc[n_rows,'Object_response'])
            
    return stocks_ticker_GET
    
    
def get_p_process_by_ticker(API_key:str,
                            client,
                            ticker:str,
                            timestamps_list:list,
                            stock_prices_frequency:str):
    
    '''
    Getting the price by specified ticker
    '''
    
    _from_ = timestamps_list.min().date()
    _to_ = timestamps_list.max().date()
    
    if stock_prices_frequency == 'daily':
              
            aggs = client.get_aggs(
                        ticker,
                        1,
                        "day",
                        _from_,
                        _to_,
                    )

    return aggs


def from_unix_time_to_timestamp(unix_time:int,
                                adjust_from_ms_to_sec:bool):
    
    '''
    Converts unix time into UTC timestamp.
    '''
    
    from datetime import datetime as dt 
    
    unix = int(unix_time)
    
    if adjust_from_ms_to_sec == True:
        ddconv_raw = dt.utcfromtimestamp(unix/pow(10,3))
        ddconv = ddconv_raw.strftime('%Y-%m-%d %H:%M:%S')
    else:
        ddconv_raw = dt.utcfromtimestamp(unix)
        ddconv = ddconv_raw.strftime('%Y-%m-%d %H:%M:%S')

    return ddconv
    

def get_serialized_price_raw_resp(self,
                            ticker:str):
    ''' 
    Serializes a row raw response, including 
    the specified stock ticker.
    '''
    
    ''' 
    Unix Msec timestamp: number seconds elapsed since 00:00:00 UTC on 1 January 1970
    less adjustment due to leap seconds. This time is converted added to the 
    serialized data.
    '''
    
    import json
    
    return {"stock":ticker,
                "financial_data": {
                        "open": self.open,
                        "high": self.high,
                        "low": self.low,
                        "close": self.close,
                        "volume": self.volume,
                        "wrap": self.vwap,
                        "timestamp": self.timestamp,
                        "converted_utc_timestamp":from_unix_time_to_timestamp(self.timestamp,
                                                                              adjust_from_ms_to_sec=True),
                        "transactions":self.transactions,
                        "otc":self.otc
                }
            }


def get_serialized_dividends_raw_resp(self,
                            ticker:str):
    ''' 
    Serializes a row raw response, including 
    the specified stock ticker for dividends.
    '''
    
    import json
    
    return {"stock":ticker,
                "Dividend": {
                        "cash_amount": self.cash_amount,
                        "declaration_date": self.declaration_date,
                        "dividend_type": self.dividend_type,
                        "ex_dividend_date": self.ex_dividend_date,
                        "frequency": self.frequency,
                        "pay_date": self.pay_date,
                        "record_date": self.record_date,
                }
            }


def get_serialized_price_raw_into_json(raw_response:list,
                                ticker:str):
    ''' 
    Takes a dump of Agg objects list and returns each row as a json
    in a jsonList.
    '''
    import json
    
    jsonList = []
    
    for raw in raw_response:
        serie = get_serialized_price_raw_resp(raw,ticker)
        
        jsonDict = json.loads(json.dumps(serie))
        
        jsonList.append(jsonDict)
        
    return jsonList


def get_jsonList_prices_into_dataframe(_json_serialized_results:list,
                                target_variables:list) -> pd.DataFrame:
    
    ''' 
    Transform the raw extracted response into a dataframe, adding
    the target variables as columns
    '''
    
    import pandas as pd
    
    df1 = pd.DataFrame.from_dict(_json_serialized_results) 

    for n_rows in range(df1.shape[0]):
        
        df1.loc[n_rows,'converted_utc_timestamp']=df1['financial_data'][n_rows]['converted_utc_timestamp']

        df1.loc[:,'converted_utc_date'] = df1.loc[:,'converted_utc_timestamp'].apply(lambda x: str(x)[0:10])
        
        for tv in target_variables:
            df1.loc[n_rows,tv]=df1['financial_data'][n_rows][tv]
        
        sel_columns = ['stock','converted_utc_timestamp','converted_utc_date'] + target_variables
        
        df2 = df1[sel_columns].copy()
    
    return df2


def get_serialized_dividends_raw_resp(self,
                            ticker:str):
    ''' 
    Serializes a row raw response, including 
    the specified stock ticker for dividends.
    '''
    
    import json
    
    return {"stock":ticker,
                "Dividend": {
                        "cash_amount": self.cash_amount,
                        "declaration_date": self.declaration_date,
                        "dividend_type": self.dividend_type,
                        "ex_dividend_date": self.ex_dividend_date,
                        "frequency": self.frequency,
                        "pay_date": self.pay_date,
                        "record_date": self.record_date,
                }
         }

def get_top_x_dividends_by_ticker(ticker:str,
                                  client,
                                  API_latency_secs:float,
                                  n_API_calls:int):
    
    '''
    Hackey way for picking up the last x dividends from the generator.
    Assuming that min frequency for dividends to be paid is monthly and that we are 
    query at most two years of daily prices, the minmax number of dividends
    I need to get is 12*2 = 24.
    Need to increase the time.sleep sufficiently to avoid being blocked by the 
    max API call rule from polygon. 
    It seems that API_latency_secs >=2 secs works.
    '''
    
    import itertools
    import time
    import json
    
    topdiv = itertools.islice(client.list_dividends(ticker=ticker), n_API_calls)

    jsonList = []
    
    for t in topdiv:
        serie = get_serialized_dividends_raw_resp(self=t,ticker=ticker)
        jsonDict = json.loads(json.dumps(serie))
        jsonList.append(jsonDict)
        time.sleep(API_latency_secs)
        
    return jsonList


def get_jsonList_dividends_into_dataframe(DivjsonList:list,
                                          measurable_time_variables:list,
                                          dividend_types:list) -> pd.DataFrame: 
    
    ''' 
    Transform the raw extracted response into a dataframe, adding
    the target variables as columns. 
    '''
    
    import pandas as pd
    
    df1 = pd.DataFrame.from_dict(DivjsonList) 
    
    for n_rows in range(df1.shape[0]):
        
        for v in ['cash_amount','dividend_type','frequency']:
            df1.loc[n_rows,v]=df1['Dividend'][n_rows][v]
        
        for mt in measurable_time_variables:
            df1.loc[n_rows,mt]=df1['Dividend'][n_rows][mt]
        
    sel_columns = ['stock','dividend_type','frequency','cash_amount'] + measurable_time_variables
        
    df2 = df1[df1['dividend_type'].isin(dividend_types)].copy().reset_index(drop=True)
    
    df3 = df2[sel_columns].copy()
            
    return df3


def get_dividend_price_ratios(ticker:str,
                              prices_df:pd.DataFrame,
                              dividends_df:pd.DataFrame,
                              dividends_aggregation_method:str) -> pd.DataFrame:
    
    ''' 
    Returns a dataframe with dividend price ratio over time.
    Dividends are aggregated over ordinary payments based on dividends_aggregation_method.
    Dividends_aggregation_method can be 'current-year-based' or 'rolling-12-m'.
    This window is daily updated. 
    '''
    
    from datetime import datetime, timedelta
    import pandas as pd 
    import datetime as dt 
    import numpy as np
    
    pdf = prices_df[prices_df['stock']==ticker].reset_index(drop=True).copy()
    ddf = dividends_df[dividends_df['stock']==ticker].reset_index(drop=True).copy()
   
    pdf['converted_utc_timestamp']= pd.to_datetime(pdf['converted_utc_timestamp'])
    ddf['pay_date']=pd.to_datetime(ddf['pay_date'])

    if dividends_aggregation_method =='current-year-based':
        
        ddf['pay_date_year']=ddf['pay_date'].apply(lambda x: x.year)
            
        for n_rows in range(pdf.shape[0]):
                
            tub_ts= pdf.loc[n_rows,'converted_utc_timestamp']
            tub = tub_ts.to_datetime64()
            tub_year = tub_ts.year
            
            if pdf[pdf['converted_utc_timestamp']==tub].index.size >0:
                rn = pdf[pdf['converted_utc_timestamp']==tub].index[0]
                __mask__ =  (ddf['pay_date']<=tub)&(ddf['pay_date_year']==tub_year)
                pdf.loc[rn,'n_paid_dividens_current_year'] = ddf[__mask__].shape[0]
                pdf.loc[rn,'sum_paid_dividens_current_year'] = ddf[__mask__].cash_amount.sum()
    
                    
        pdf['DP_ratio']=pdf['sum_paid_dividens_current_year']*100/pdf['close']
                
        aggr_df = pdf[['stock','close','converted_utc_timestamp',
                    'n_paid_dividens_current_year','sum_paid_dividens_current_year','DP_ratio']]
    
    elif dividends_aggregation_method == 'rolling-12-m':
        
        for n_rows in range(pdf.shape[0]):
            
            tub_ts= pdf.loc[n_rows,'converted_utc_timestamp']
            tlb_ts = tub_ts + timedelta(days=-365)
            tub, tlb = tub_ts.to_datetime64(),tlb_ts.to_datetime64()
            
            if pdf[pdf['converted_utc_timestamp']==tub].index.size >0:
                rn = pdf[pdf['converted_utc_timestamp']==tub].index[0]
                __mask__ =  (ddf['pay_date']<=tub)&(ddf['pay_date']>=tlb)
                pdf.loc[rn,'n_paid_dividens_rolling12m'] = ddf[__mask__].shape[0]
                pdf.loc[rn,'sum_paid_dividens_rolling12m'] = ddf[__mask__].cash_amount.sum()
                
        pdf['DP_ratio']=pdf['sum_paid_dividens_rolling12m']*100/pdf['close']
        
        aggr_df = pdf[['stock','close','converted_utc_timestamp',
                    'n_paid_dividens_rolling12m','sum_paid_dividens_rolling12m','DP_ratio']]
        
    aggr_df = aggr_df.sort_values(by=['converted_utc_timestamp'],ascending=False).reset_index(drop=True)

    return aggr_df


def zero_prefixing(check_var:str):

    '''
    Take a string, convert to integer and prefixes a zero 
    if integer is less than 10.
    '''
     
    if check_var < 10:
        check_var_st = "0"+ str(check_var)
    else:
        check_var_st = str(check_var)
        
    return check_var_st


def DoNotifyFlag(ds_dp: pd.DataFrame,
                stocks_requirments: pd.DataFrame,
                ticker:str,
                dividends_aggregation_method:str) -> pd.DataFrame:
    
    ''' 
    It takes the DP_ratio dataset and compare the latest 
    DP_ratio with the warning levels per stock, given back 
    for which stock the alert needs to be triggered.
    '''
    
    import numpy as np
    
    AlertData = {'ticker':"",
                   'current_level':"",
                   'alert_level':"",
                   'close':"",
                   'cumulative_dividends':"",
                   'data_latest_timestamp':"",
                   'dividends_aggregation_method':"",
                   'flag_alert':""}
    
    last_date = ds_dp[ds_dp['stock']==ticker]['converted_utc_timestamp'].max()
    y,mon,d= last_date.year,zero_prefixing(last_date.month),zero_prefixing(last_date.day)
    h,min,s=zero_prefixing(last_date.hour),zero_prefixing(last_date.minute),zero_prefixing(last_date.second)
    
    __when__ = str(y)+"-"+str(mon)+"-"+str(d)+" " + str(h)+":"+str(min)+":"+str(s)
    
    
    alp = float(stocks_requirments[stocks_requirments['ticker']==ticker].alert_level_percentage.reset_index(drop=True)[0])*100 
    dp_s=ds_dp[(ds_dp['stock']==ticker)&(ds_dp['converted_utc_timestamp']==last_date)]
    
    if dividends_aggregation_method == 'rolling-12-m':
        cumulative_dividends = np.round(dp_s['sum_paid_dividens_rolling12m'][0],4)
    
    elif dividends_aggregation_method == 'current-year-based':
        cumulative_dividends = np.round(dp_s['sum_paid_dividens_current_year'][0],4)
        
        
    if dp_s.DP_ratio[0] > alp:
    
            AlertData.update({'ticker':ticker,
                                'current_level':np.round(dp_s.DP_ratio[0],4),
                                'alert_level':alp,
                                'close':np.round(dp_s.close[0],2),
                                'cumulative_dividends':cumulative_dividends,
                                'data_latest_timestamp':__when__,
                                'dividends_aggregation_method':dividends_aggregation_method,
                                'flag_alert':True
                                })
    else:
            AlertData.update({'ticker':ticker,
                                'current_level':np.round(dp_s.DP_ratio[0],4),
                                'alert_level':alp,
                                'close':np.round(dp_s.close[0],2),
                                'cumulative_dividends':cumulative_dividends,
                                'data_latest_timestamp':__when__,
                                'dividends_aggregation_method':dividends_aggregation_method,
                                'flag_alert':False
                                })
    
    print(ticker)
    
    return AlertData


def slackify(webhook_url,
            ticker:str,
            AlertData):

    ''' 
    Send a slack message in channel to notify if a stock is over the limit.
    '''
    
    import requests
    import json 
    
    if AlertData['flag_alert']==True:
    
        alert_body={
        "blocks": [
            {
                "type": "divider"
            },
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "ALERT REACHED for stock " + AlertData['ticker']
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":red_circle: Current direktavkastning: " + str(AlertData['current_level'])+"%"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":robot_face: Alert trigger: " + str(AlertData['alert_level'])+"%"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":chart: Stock price: " + str(AlertData['close'])
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":moneybag: Cumulative ordinary dividends: " + str(AlertData['cumulative_dividends'])
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":calendar: Latest data timestamp: " + AlertData['data_latest_timestamp']
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": "Method for cumulating dividends: "+ AlertData['dividends_aggregation_method']
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
    }
    else:
         alert_body={
        "blocks": [
            {
                "type": "divider"
            },
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Stock " + AlertData['ticker']
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "*Alert not reached*"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":red_circle: Current direktavkastning: " + str(AlertData['current_level'])+"%"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":robot_face: Alert trigger: " + str(AlertData['alert_level'])+"%"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":chart: Stock price: " + str(AlertData['close'])
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":moneybag: Cumulative ordinary dividends: " + str(AlertData['cumulative_dividends'])
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": ":calendar: Latest data timestamp: " + AlertData['data_latest_timestamp']
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": "Method for cumulating dividends: "+ AlertData['dividends_aggregation_method']
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
    }
         
    response = requests.post(webhook_url,
                            data =json.dumps(alert_body))
    
    print(response.status_code,response.text)
        
    return response.status_code,response.text
    