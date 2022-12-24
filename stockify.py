'''
Set of function to import financial data from API 
'''
import pandas as pd 

def get_list_stocks_tickers(text_file_name: str) -> list:
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
        stock_tickers = list(df.iloc[:,1]) 
    
    return stock_tickers

def get_daily_time_buckets(max_lookback_years:int,
                           current_is_timestamp:bool,
                           frequency:str,
                           include_weekend:bool 
                           ) -> pd.Series: 
    
    ''' 
    Get a range of discrete time points, given a current
    time as final data point. Frequency: daily or monthly.
    '''
    
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

def get_p_process_by_ticker(ticker:str,
                            timestamps_list:list,
                            stock_prices_frequency:str):
    
    '''
    Getting the price by specified ticker
    '''
  
    from polygon import RESTClient
    from polygon.rest import models
    
    API_key = "PuJHla6_U_GDHcNE3XSpk7L2gCCpdJ9J"
    client = RESTClient(API_key)
    
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
    

def get_serialized_raw_resp(self,
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

def get_serialized_raw_into_json(raw_response:list,
                                ticker:str):
    ''' 
    Takes a dump of Agg objects list and returns each row as a json
    in a jsonList.
    '''
    
    jsonList = []
    
    for raw in raw_response:
        serie = get_serialized_raw_resp(raw,ticker)
        
        jsonDict = json.loads(json.dumps(serie))
        
        jsonList.append(jsonDict)
        
    return jsonList

def get_jsonList_into_dataframe(jsonList:list) -> pd.DataFrame:
    
    ''' 
    Transform list into a dataframe
    '''
    
    import pandas as pd 
    
    return jsonList