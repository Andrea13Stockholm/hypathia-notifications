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
                                  number_dividends_back_in_time:int,
                                  API_latency_secs:float):
    
    '''
    Hackay way for picking up the last x dividends from the generator.
    Assuming that min frequency of payment is month, then by setting
    the parameter number_dividends_back_in_time to 12 should pick up the last
    year dividends. API calls are delay by API_latency_secs
    '''
    
    import itertools
    import time
    import json
    
    topdiv = itertools.islice(client.list_dividends(ticker=ticker), number_dividends_back_in_time)

    jsonList = []
    
    for t in topdiv:
        serie = get_serialized_dividends_raw_resp(self=t,ticker=ticker)
        jsonDict = json.loads(json.dumps(serie))
        jsonList.append(jsonDict)
        time.sleep(API_latency_secs)
        
    return jsonList


def get_jsonList_dividends_into_dataframe(DivjsonList:list,
                                          measurable_time_variables:list) -> pd.DataFrame: 
    
    ''' 
    Transform the raw extracted response into a dataframe, adding
    the target variables as columns. 
    '''
    
    import pandas as pd
    
    df1 = pd.DataFrame.from_dict(DivjsonList) 
    
    for n_rows in range(df1.shape[0]):
    
        df1.loc[n_rows,'cash_amount']=df1['Dividend'][n_rows]['cash_amount']
        df1.loc[n_rows,'dividend_type']=df1['Dividend'][n_rows]['dividend_type']
        df1.loc[n_rows,'frequency']=df1['Dividend'][n_rows]['frequency']
    
        for mt in measurable_time_variables:
            df1.loc[n_rows,mt]=df1['Dividend'][n_rows][mt]
        
    sel_columns = ['stock','dividend_type','frequency','cash_amount'] + measurable_time_variables
        
    df2 = df1[sel_columns].copy()
        
    return df2


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
    
    
def get_last_x_days_dividend_dataframe(Div_df:pd.DataFrame, 
                                       number_days_looking_back:int,
                                       measureble_time_variable:str) -> pd.DataFrame:

    ''' 
    Get as input the dataframe with dividends and spits out the
    last number_days_looking_back days dividend flows.
    '''
    
    import datetime as dt 
    from datetime import datetime, timedelta
    
    current_date = dt.datetime.now().date()
    current_date_shifted = current_date + timedelta(days=-number_days_looking_back)
    
    ''' 
    Here I need to think about the case when there are no dividends at all, as an
    expection, but this can be handled when I go aggregate.
    '''
    _tb_ = current_date_shifted
    y, m, d = _tb_.year,zero_prefixing(_tb_.month),zero_prefixing(_tb_.day)
    
    current_date_shifted_filt = str(y)+"-"+str(m)+"-"+str(d)
    
    df3 = Div_df[Div_df.loc[:,measureble_time_variable]>=current_date_shifted_filt].reset_index(drop=True).copy()
    
    df3['current_date']=current_date_shifted_filt

    return df3