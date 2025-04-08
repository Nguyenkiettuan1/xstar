import numpy as np
import pandas as pd

from handle_data import get_data
import os



# def normalize_value(val):
#     if pd.isna(val):
#         return "NA"  # Giá trị thiếu sẽ chuyển thành 'NA'
#     try:
#         val = float(val)
#         if val.is_integer():
#             return str(int(val))  # Loại bỏ ".0" nếu là số nguyên
#         return str(val)
#     except:
#         return str(val).strip()  # Chuẩn hóa chuỗi


def compare_dataframes(df1, df2, column_map, output_file="/logs/data.csv"):
    """
    Kiểm tra xem các dòng từ df1 có tồn tại trong df2 dựa trên ánh xạ cột và ghi log nếu không tồn tại.
    
    Parameters:
    - df1 (DataFrame): DataFrame cần kiểm tra.
    - df2 (DataFrame): DataFrame dùng để đối chiếu.
    - column_map (dict): Ánh xạ giữa cột của df1 và df2.
    - output_file (str): Đường dẫn file log (CSV) nếu có dòng không tìm thấy.
    
    Returns:
    - bool: True nếu tất cả các dòng trong df1 tồn tại trong df2, False nếu có dòng không tồn tại và đã ghi log.
    """
    # Chuyển tất cả các cột về dạng chuỗi để tránh lỗi kiểu dữ liệu

    df1 = df1.astype(str)
    df2 = df2.astype(str)
    df1 = df1.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df2 = df2.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    
    not_found = []  # Danh sách các dòng không tìm thấy

    # So sánh từng dòng của df1 với df2 dựa trên ánh xạ cột
    for _, row in df1.iterrows():
        # Tạo một DataFrame tạm từ dòng hiện tại và ánh xạ cột
        temp_row = pd.DataFrame([row[list(column_map.keys())].values], columns=list(column_map.keys()))
        temp_row.columns = [column_map[col] for col in temp_row.columns]  # Ánh xạ tên cột sang cột của df2
       
        # Kiểm tra xem dòng này có tồn tại trong df2 không
      
        
        if not ((df2[temp_row.columns] == temp_row.iloc[0]).all(axis=1)).any():
            similar_rows = df2[
                (df2['transaction_id'] == row['TransactionID']) & 
                (df2['security_id'] == row['SecurityID']) & 
                (df2['security_id_type'] == row['SecurityIDType']) & 
                (df2['date_time_trade'] == row['TradeDate']) & 
                (df2['settle_date'] == row['SettleDateTarget']) & 
                (df2['trade_price'] == row['TradePrice']) & 
                (df2['cost_basis'] == row['CostBasis']) & 
                (df2['commission'] == row['IBCommission']) & 
                (df2['order_type'] == row['OrderType']) & 
                (df2['cusip'] == row['CUSIP']) & 
                (df2['isin'] == row['ISIN']) & 
                (df2['figi'] == row['FIGI']) & 
                (df2['quantity'] == row['Quantity'])
            ]
            # print(df2)
            # print(similar_rows)
            # print(f"Dòng {row} không tồn tại trong df2.")
            # print(temp_row)      
            # return False       
            not_found.append(row)
    

    
    # Ghi log vào file CSV nếu có dòng không tồn tại
    if not_found:
        not_found_df = pd.DataFrame(not_found)
        output_dir = os.path.dirname(output_file)
        
        # Kiểm tra và tạo thư mục nếu chưa tồn tại
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        not_found_df.to_csv(output_file, index=False)
        print(f"Có {len(not_found)} dòng không tồn tại trong df2. Đã ghi log vào {output_file}.")
        return False
    else:
        print("Tất cả các dòng của df1 đều tồn tại trong df2.")
        return True







security_holding_raw = get_data(path='table/security_holding_raw_table.csv')
security_raw = get_data(path='table/security_raw_table.csv')  
trades_raw = get_data(path='table/trades_raw_table.csv')




security_table = get_data(path='data/security.csv')
security_holding_table = get_data(path='data/sercurities_holdings.csv')
trades = get_data(path='data/trades.csv')

# data in table security
# print('field in security table:', end='\n\n')
# print(security_table.columns.values,end= '\n\n')

# print('field in security_holding_table table:', end='\n\n')
# print(security_holding_table.columns.values,end= '\n\n')

# print('field in trades table:', end='\n\n')
# print(trades.columns.values,end= '\n\n')

# print('field in security_raw table:', end='\n\n')
# print(security_raw.columns.values)

# print('field in security_holding_raw table:', end='\n\n')
# print(security_holding_raw.columns.values)

# print('field in trades_raw table:', end='\n\n')
# print(trades_raw.columns.values)



# Take the necessary columns from the security_table table for comparison.
data_security_in = security_table.loc[:, ['self_generated_id', 'security_id','security_id_type','isin','figi','cusip']].copy()

# Take the necessary columns from the security_holding table for comparison.
security_holding_in = security_holding_table.iloc[:, 1:].copy()
trades_in = trades.iloc[:, 1:].copy()

print(trades_in[trades_in['cost_basis'] == -10820.20972])


#merge the two tables to get the common columns and compare them with data raw tables
combined_data = pd.merge(security_holding_in,data_security_in, left_on='dim_security_id', right_on='self_generated_id')
combined_data_trade_table = pd.merge(trades_in, data_security_in, left_on='dim_security_id', right_on='self_generated_id')



combined_data_compare = combined_data.loc[:, ['side', 'security_id', 'security_id_type', 'open_date_time', 'cost_basis', 'holding_period_date', 'quantity', 'cusip', 'isin', 'figi']].copy()



combined_data_compare['open_date_time'] = pd.to_datetime(combined_data_compare['open_date_time'], errors='coerce').dt.date
combined_data_compare['holding_period_date'] = pd.to_datetime(combined_data_compare['holding_period_date'], errors='coerce').dt.date



security_holding_raw_compare = security_holding_raw.loc[:, ['Side', 'SecurityID', 'SecurityIDType','OpenDateTime','CostBasisPrice','HoldingPeriodDateTime','Quantity', 'CUSIP' ,'ISIN' ,'FIGI']].copy()
# print(security_holding_raw_compare[security_holding_raw_compare['SecurityID'] == 'KYG2118N1079'])

security_holding_raw_compare['OpenDateTime'] = security_holding_raw_compare['OpenDateTime'].str.split(' ').str[0]
security_holding_raw_compare['OpenDateTime'] = pd.to_datetime(
    security_holding_raw_compare['OpenDateTime'], errors='coerce'
).dt.date

security_holding_raw_compare['HoldingPeriodDateTime'] = security_holding_raw_compare['HoldingPeriodDateTime'].str.split(' ').str[0]
security_holding_raw_compare['HoldingPeriodDateTime'] = pd.to_datetime(
    security_holding_raw_compare['HoldingPeriodDateTime'], errors='coerce'
).dt.date




# Mapping columns between raw data and combined data
column_map_security_holding = {
    'Side': 'side',
    'SecurityID': 'security_id',
    'SecurityIDType': 'security_id_type',
    'OpenDateTime': 'open_date_time',
    'CostBasisPrice': 'cost_basis',
    'HoldingPeriodDateTime': 'holding_period_date',
    'Quantity': 'quantity',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'FIGI': 'figi'
}



# print(combined_data_compare.columns.values)

# result = combined_data_compare[
#     (combined_data_compare['figi'] == 'BBG01LPSZ6P4') & 
#     (combined_data_compare['open_date_time'] == '2024-06-28') 
# ]
# print(result)


# result1 = security_holding_raw_compare[(security_holding_raw_compare['SecurityID'] == 'KYG2118N1079') & (security_holding_raw_compare['SecurityIDType'] == 'ISIN')]
# print(result1)



# result = compare_dataframes(security_holding_raw_compare,combined_data_compare , column_map_security_holding, output_file="logs/security_holding.csv")
# print("Tất cả các dòng tồn tại trong combined_data:", result)





# print(combined_data_trade_table.columns.values, end= '\n\n')


combined_data_trade_table = combined_data_trade_table.loc[:,['transaction_id','open_close','quantity','date_time_trade','settle_date','trade_price','taxes',
 'commission', 'cost_basis','order_type','security_id',
 'security_id_type','isin','figi','cusip']]




combined_data_trade_table['date_time_trade'] = pd.to_datetime(combined_data_trade_table['date_time_trade'], errors='coerce').dt.date
combined_data_trade_table['settle_date'] = pd.to_datetime(combined_data_trade_table['settle_date'], errors='coerce').dt.date

# print(combined_data_trade_table.columns.values, end= '\n\n')

combined_data_trades_raw = trades_raw.loc[:, ['Open/CloseIndicator', 'SecurityID', 'SecurityIDType','Quantity','TransactionID','TradeDate','SettleDateTarget','TradePrice','CostBasis','IBCommission','OrderType' ,'CUSIP' ,'ISIN' ,'FIGI']].copy()
# print(combined_data_trades_raw,end= '\n\n')

combined_data_trades_raw['TradeDate'] = combined_data_trades_raw['TradeDate'].str.split(' ').str[0]
combined_data_trades_raw['TradeDate'] = pd.to_datetime(
    combined_data_trades_raw['TradeDate'], errors='coerce'
).dt.date

combined_data_trades_raw['SettleDateTarget'] = combined_data_trades_raw['SettleDateTarget'].str.split(' ').str[0]
combined_data_trades_raw['SettleDateTarget'] = pd.to_datetime(
    combined_data_trades_raw['SettleDateTarget'], errors='coerce'
).dt.date

# Mapping columns between raw data and combined data
column_map_trades = {
    'Open/CloseIndicator': 'open_close',
    'SecurityID': 'security_id',
    'SecurityIDType': 'security_id_type',
    'Quantity': 'quantity',
    'TransactionID': 'transaction_id',
    'TradeDate': 'date_time_trade',
    'SettleDateTarget': 'settle_date',
    'TradePrice': 'trade_price',
    'CostBasis': 'cost_basis',
    'IBCommission': 'commission',
    'OrderType': 'order_type',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'FIGI': 'figi'
}
print(combined_data_trade_table[combined_data_trade_table['cost_basis'] == -10820.20972]['cost_basis'], end= '\n\n')




# combined_data_trades_raw = combined_data_trades_raw.astype(str)
# combined_data_trade_table = combined_data_trade_table.astype(str)

# print(combined_data_trade_table[combined_data_trade_table['transaction_id'] == '28081906451'], end= '\n\n')

# combined_data_trades_raw['TransactionID'] = combined_data_trades_raw['TransactionID'].str.split('.').str[0]
# combined_data_trades_raw['Quantity'] = combined_data_trades_raw['Quantity'].str.split('.').str[0]

# combined_data_trade_table['quantity'] = combined_data_trade_table['quantity'].str.split('.').str[0]

# result = compare_dataframes(combined_data_trades_raw, combined_data_trade_table, column_map_trades, output_file="logs/trades.csv")
# print("Tất cả các dòng tồn tại trong combined_data_trade_table:", result)

# print(combined_data_trades_raw[combined_data_trades_raw['CostBasis'] =='-10820.20972' ], end= '\n\n')
# print(combined_data_trade_table[combined_data_trade_table['cost_basis'] == '-10820.209722'], end= '\n\n')

# print(combined_data_trades_raw['Quantity'])
# print(combined_data_trade_table['quantity'])









raw = get_data(path='data/A02-Trades-Details 2024-06.csv',start_row=0,end_row=2487)

raw2 = get_data(path='data/Last_Month_Trades_-_2024-05.csv',start_row=0,end_row=1709)

column_map_trades = {
    'Open/CloseIndicator': 'open_close',
    'SecurityID': 'security_id',
    'SecurityIDType': 'security_id_type',
    'Quantity': 'quantity',
    'TransactionID': 'transaction_id',
    'TradeDate': 'date_time_trade',
    'SettleDateTarget': 'settle_date',
    'TradePrice': 'trade_price',
    'CostBasis': 'cost_basis',
    'IBCommission': 'commission',
    'OrderType': 'order_type',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'FIGI': 'figi'
}
check = compare_dataframes(raw,raw2,column_map_trades,'logs/sameTranscation_id')
