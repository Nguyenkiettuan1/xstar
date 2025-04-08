import numpy as np
import pandas as pd


def get_data(list_columns = None, path = None,start_row = None, end_row = None, coulums_start = None, columns_end = None): 
    data = pd.read_csv(path,float_precision='round_trip')
    if list_columns:
        data =  data[list_columns]
    if start_row != None  :
        if end_row == None:
            data = data[start_row:]
        else:
            data = data[start_row:end_row]
    if coulums_start != None  :
        if columns_end == None:
            data = data.iloc[:, coulums_start:]
        else:
            data = data.iloc[:, coulums_start:columns_end]
    return data.reset_index(drop=True)


                            
security_holding = get_data(path='data/A01-Open_Positions-2024-06.csv' , start_row=0, end_row=7799)
security_holding.to_csv('table/security_holding_raw_table.csv', index=False)

security = get_data(path='data/A01-Open_Positions-2024-06.csv' , start_row=7799, coulums_start=0, columns_end=23)
security.to_csv('table/security_raw_table.csv', index=False)


trades_raw = get_data(path='data/A02-Trades-Details-2024-06.csv',start_row= 0, end_row= 2487)
trades_raw.to_csv('table/trades_raw_table.csv', index=False)