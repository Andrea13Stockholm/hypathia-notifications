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
                                  API_latency_secs:float,
                                  n_API_calls:int):
    
    '''
    Hackey way for picking up the last x dividends from the generator.
    Assuming that min frequency for dividends to be paid is monthly and that we are 
    query at most two years of daily prices, the max number of dividends
    I need to get is 12*12 = 144.
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
                                          measurable_time_variables:list) -> pd.DataFrame: 
    
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
        
    df2 = df1[sel_columns].copy()
        
    return df2


def get_dividend_price_ratios(ticker:str,
                              prices_df:pd.DataFrame,
                              dividends_df:pd.DataFrame) -> pd.DataFrame:
    
    ''' 
    Returns a dataframe with dividend price ratio over time.
    Dividends are aggregated over 12 months. This window is daily updated
    by shifting each observed price date t by 12 months and summing all 
    dividends paids and falling in this (daily) interval.
    '''
    
    from datetime import datetime, timedelta
    import pandas as pd 
    import datetime as dt 
    
    pdf = prices_df[prices_df['stock']==ticker].reset_index(drop=True).copy()
    ddf = dividends_df[dividends_df['stock']==ticker].reset_index(drop=True).copy()
    
    pdf['converted_utc_timestamp']= pd.to_datetime(pdf['converted_utc_timestamp'])
    ddf['pay_date']=pd.to_datetime(ddf['pay_date'])
    
    pdf['converted_utc_timestamp_back_shifted']=pdf['converted_utc_timestamp'].apply(lambda x: x+timedelta(days=-365))
    
    for n_rows in range(pdf.shape[0]):
        
        tub,tlb = pdf.loc[n_rows,'converted_utc_timestamp'],pdf.loc[n_rows,'converted_utc_timestamp_back_shifted']
         
        if pdf[pdf['converted_utc_timestamp']==tub].index.size >0:
            rn = pdf[pdf['converted_utc_timestamp']==tub].index[0]
            pdf.loc[rn,'n_paid_dividens_12_m'] = ddf[(ddf['pay_date']>=tlb)&(ddf['pay_date']<=tub)].shape[0]
            pdf.loc[rn,'sum_paid_dividens_12_m'] = ddf[(ddf['pay_date']>=tlb)&(ddf['pay_date']<=tub)].cash_amount.sum()
    
    pdf['DP_ratio']=pdf['sum_paid_dividens_12_m']/pdf['close']
    

    aggr_df = pdf[['stock','close','converted_utc_timestamp','converted_utc_timestamp_back_shifted',
                   'n_paid_dividens_12_m','sum_paid_dividens_12_m','DP_ratio']]
    
    aggr_df = aggr_df.sort_values(by=['converted_utc_timestamp'],ascending=False).reset_index(drop=True)
    
    return aggr_df 