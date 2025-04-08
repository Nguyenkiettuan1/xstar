import pandas as pd


def read_data_excel(path,header= None,sheet_name=None) -> pd.DataFrame:
    return  pd.read_excel(path,header= header,sheet_name= sheet_name)

def read_csv(path,header= None) -> pd.DataFrame:
    return pd.read_csv(path,header= header)


def get_data(list_columns=None, path=None, start_row=None, end_row=None, coulums_start=None, columns_end=None):
    data = pd.read_csv(path, float_precision='round_trip')
    if list_columns:
        data = data[list_columns]
    if start_row != None:
        if end_row == None:
            data = data[start_row:]
        else:
            data = data[start_row:end_row]
    if coulums_start != None:
        if columns_end == None:
            data = data.iloc[:, coulums_start:]
        else:
            data = data.iloc[:, coulums_start:columns_end]
    return data.reset_index(drop=True)


