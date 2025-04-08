import asyncio
from datetime import date

import asyncpg
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os
import database as db



load_dotenv()

dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")



def read_data_excel(path,header= None,sheet_name=None):
    return  pd.read_excel(path,header= header,sheet_name= sheet_name)
def read_csv(path,header= None):
    return pd.read_csv(path,header= header)


async def process_data_trades(raw_data, df_old : pd.DataFrame = None) -> pd.DataFrame | None:
    # Nếu có sell thì mảng này lưu những giao dịch đã bán hết

    def calculate_open_close(row):
        if ((row['Mua/Bán\n(Buy/Sell)'] == "Buy" and row.get('side', 'Long') == "Long") or
                (row['Mua/Bán\n(Buy/Sell)'] == "Div" and row.get('side', 'Long') == "Long") or
                (row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row.get('side', 'Long') == "Short")):
            return "O"
        elif ((row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row.get('side', 'Long') == "Long") or
              (row['Mua/Bán\n(Buy/Sell)'] == "Buy" and row.get('side', 'Long') == "Short")):
            return "C"
        else:
            return ""

    def calculate_quantily(row):
        if ((row['Mua/Bán\n(Buy/Sell)'] == "Buy" or row['Mua/Bán\n(Buy/Sell)'] == "Div")):
            return row['KL khớp\n(Matched Quantity)']
        else:
            if (row['Mua/Bán\n(Buy/Sell)'] == "Sell"):
                return -row['KL khớp\n(Matched Quantity)']
        return -1

    def calculate_value(rows,refdata):
        temp = pd.DataFrame(columns=['settle_date'])  # Khởi tạo temp với cột 'settle_date'

        settle_dates = []  # Sử dụng danh sách tạm để lưu các giá trị 'settle_date'

        for _, row in rows.iterrows():  # Lặp qua từng dòng
            date = row['date']

            if row['Mua/Bán\n(Buy/Sell)'] == "Div":
                settle_dates.append(date)
            else:
                try:
                    # First lookup: Find the row where 'date' matches
                    match1 = refdata[refdata['date'] == date].index
                    if len(match1) > 0:
                        lookup_value = match1[0] + 2

                        # Second lookup: Ensure lookup_value is within bounds
                        if lookup_value < len(refdata):
                            match2 = refdata.iloc[lookup_value]['date']
                            settle_dates.append(match2)
                        else:
                            settle_dates.append(np.nan)
                except Exception as e:
                    print(f"Lỗi: {e}")

        temp['settle_date'] = settle_dates  # Gán danh sách 'settle_dates' vào cột 'settle_date'
        return temp
    def calculate_Tax(row):
        if ((row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row['side'] == "Long")):
            proceeds = row['Giá khớp\n(Matched Price)'] * row['KL khớp\n(Matched Quantity)']
            tax = -proceeds * 0.001  # 0.1% = 0.001
            return tax
        else:
            return 0

    def cosbasic(row):
        if row['open_close'] == 'O':
            return -row['NetCash']
        elif row['open_close'] == 'C' and row['Giá khớp\n(Matched Price)'] == "":
            # Tìm giá trị VLOOKUP cho BR + "O" trong trades
            lookup_key = row['transaction_id']

            bm_value = row.loc[raw_data['Số hiệu lệnh\n(Order ID)'] == lookup_key,'NetCash' ].values
            at_value = row.loc[raw_data['Số hiệu lệnh\n(Order ID)'] == lookup_key, 'KL khớp\n(Matched Quantity)' ].values

            if len(bm_value) > 0 and len(at_value) > 0 and at_value[0] != 0:
                return (bm_value[0] / at_value[0]) * row['KL khớp\n(Matched Quantity)']
            else:
                return np.nan
        else:
            return np.nan

    def process_trade(trades_data: pd.DataFrame) -> pd.DataFrame:
        # Tạo bản sao của DataFrame và reset index
        trades = trades_data.copy().reset_index(drop=True)

        # Thêm cột 'remain' với giá trị ban đầu bằng quantity
        trades['remain'] = trades['quantity'].copy()

        # Chuẩn hóa Symbol
        trades['symbol'] = trades['symbol'].astype(str).str.strip().str.upper()

        buy_trades = {}
        for idx, row in trades.iterrows():
            if row.get('open_close') == "O":
                buy_trades[idx] = row.to_dict()
                buy_trades[idx]['remain'] = row['quantity']  # Khởi tạo remain trong buy_trades

        result_trades = trades.copy()
        rows_added = 0  # Theo dõi số hàng đã thêm để điều chỉnh chỉ mục

        for idx, row in trades.iterrows():
            if row.get('open_close') == "C":
                sell_symbol = row['symbol']
                sell_qty = abs(row['quantity'])  # Chuyển quantity bán thành dương
                current_new_rows = []  # Lưu các hàng mới cho giao dịch close hiện tại

                for buy_idx in sorted(buy_trades.keys()):
                    if sell_qty <= 0:
                        break
                    buy_trade = buy_trades[buy_idx]
                    if buy_trade['symbol'] != sell_symbol or buy_trade['remain'] <= 0:
                        continue

                    available = buy_trade['remain']

                    if available <= sell_qty:
                        sell_qty -= available
                        new_row = {col: buy_trades[buy_idx][col] for col in trades.columns if col != 'remain'}
                        new_row['open_close'] = row['open_close']
                        new_row['transaction_id'] = str(buy_trade['transaction_id'])
                        new_row['quantity'] = available
                        new_row['relatedtransactionid'] = buy_trade['transaction_id']
                        new_row['remain'] = 0  # Dòng mới có remain = 0
                        buy_trades[buy_idx]['remain'] = 0

                        # Cập nhật remain trong result_trades
                        adjusted_idx = buy_idx + (buy_idx >= idx) * rows_added
                        result_trades.at[adjusted_idx, 'remain'] = 0

                        current_new_rows.append(new_row)
                    else:
                        available -= sell_qty
                        new_row = {col: buy_trades[buy_idx][col] for col in trades.columns if col != 'remain'}
                        new_row['open_close'] = row['open_close']
                        new_row['transaction_id'] = buy_trade['transaction_id']
                        new_row['quantity'] = sell_qty
                        new_row['relatedtransactionid'] = str(buy_trade['transaction_id'])
                        new_row['remain'] = available  # Dòng mới có remain = số lượng còn lại
                        buy_trades[buy_idx]['remain'] = available

                        # Cập nhật remain trong result_trades
                        adjusted_idx = buy_idx + (buy_idx >= idx) * rows_added
                        result_trades.at[adjusted_idx, 'remain'] = available

                        sell_qty = 0
                        current_new_rows.append(new_row)

                # Thêm các dòng mới ngay sau dòng close hiện tại
                if current_new_rows:
                    insert_idx = idx + rows_added + 1  # +1 để chèn sau dòng hiện tại
                    new_df = pd.DataFrame(current_new_rows)

                    # Chèn các dòng mới vào vị trí ngay sau dòng close
                    result_trades = pd.concat([
                        result_trades.iloc[:insert_idx],
                        new_df,
                        result_trades.iloc[insert_idx:]
                    ]).reset_index(drop=True)

                    rows_added += len(current_new_rows)

        return result_trades

    def process_trade_load_n(trades_data: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        # Tạo bản sao của DataFrame và reset index
        trades = trades_data.copy().reset_index(drop=True)
        # Thêm cột 'remain' với giá trị ban đầu bằng quantity
        trades['remain'] = trades['quantity'].copy()
        # Lọc dữ liệu cũ (df_old) dựa trên remain > 0 và open_close = 'O'
        df_old = df[(df['remain'] > 0) & (df['open_close'] == 'O')].copy()

        # Chuẩn hóa symbol cho cả trades và df_old
        trades['symbol'] = trades['symbol'].astype(str).str.strip().str.upper()
        df_old['symbol'] = df_old['symbol'].astype(str).str.strip().str.upper()

        # Tạo dictionary lưu các giao dịch 'Mua' từ dữ liệu cũ
        buy_trades = {}
        for idx, row in df_old.iterrows():
            if row.get('open_close') == "O":
                buy_trades[idx] = row.to_dict()
                buy_trades[idx]['remain'] = row['remain']

        # Lưu các giao dịch 'Mua' từ trades_data (dữ liệu mới)
        for idx, row in trades.iterrows():
            if row.get('open_close') == "O":
                # Dùng chỉ số mới để tránh trùng lặp với df_old
                new_idx = idx + len(df_old)
                buy_trades[new_idx] = row.to_dict()
                buy_trades[new_idx]['remain'] = row['quantity']

        # Khởi tạo danh sách trade_output
        trade_output = []

        # Duyệt qua từng dòng trong trades (theo thứ tự ban đầu)
        for idx, row in trades.iterrows():
            # Thêm dòng gốc (danh sách các giao dịch bán và mua từ trades_data) vào trade_output
            trade_output.append(row.to_dict())

            # Nếu là giao dịch bán (open_close = "C") thì xử lý khớp giao dịch mua
            if row.get('open_close') == "C":
                sell_symbol = row['symbol']
                sell_qty = abs(row['quantity'])  # Chuyển quantity bán thành dương
                current_new_rows = []  # Danh sách tạm lưu các dòng mới tạo ra

                # Duyệt qua các giao dịch mua theo thứ tự (FIFO)
                for buy_idx in sorted(buy_trades.keys()):
                    if sell_qty <= 0:
                        break
                    buy_trade = buy_trades[buy_idx]
                    if buy_trade['symbol'] != sell_symbol or buy_trade['remain'] <= 0:
                        continue

                    available = buy_trade['remain']

                    if available <= sell_qty:
                        sell_qty -= available
                        new_row = {col: buy_trade.get(col, "") for col in trades.columns if col != 'remain'}
                        new_row['open_close'] = row['open_close']
                        new_row['transaction_id'] = str(buy_trade['transaction_id'])
                        new_row['quantity'] = available
                        new_row['relatedtransactionid'] = buy_trade['transaction_id']
                        new_row['remain'] = 0  # Đã khớp hết
                        buy_trades[buy_idx]['remain'] = 0
                        current_new_rows.append(new_row)
                    else:
                        # Khớp một phần
                        new_qty = sell_qty
                        available = available - sell_qty
                        new_row = {col: buy_trade.get(col, "") for col in trades.columns if col != 'remain'}
                        new_row['open_close'] = row['open_close']
                        new_row['transaction_id'] =str(buy_trade['transaction_id'])
                        new_row['quantity'] = new_qty
                        new_row['relatedtransactionid'] = buy_trade['transaction_id']
                        new_row['remain'] = available  # Số lượng còn lại sau khi khớp
                        buy_trades[buy_idx]['remain'] = available
                        sell_qty = 0
                        current_new_rows.append(new_row)

                # Sau khi xử lý giao dịch bán, thêm các dòng mới tạo ra vào trade_output ngay sau dòng bán hiện tại
                for new_row in current_new_rows:
                    trade_output.append(new_row)

        # Chuyển danh sách trade_output thành DataFrame kết quả
        result_df = pd.DataFrame(trade_output)
        return result_df

    # Phần còn lại của code giữ nguyên
    trades = pd.DataFrame()
    trades['transaction_id'] = raw_data['Số hiệu lệnh\n(Order ID)'].copy()
    trades['open_close'] = raw_data.apply(calculate_open_close, axis=1)
    trades['symbol'] = raw_data['Mã chứng khoán\n(Stock code)'].copy()
    trades['side'] = 'Long'
    trades['quantity'] = raw_data.apply(calculate_quantily, axis=1)
    trades['date_time_trade'] = raw_data['Ngày giao dịch\n(Transaction Date)'].copy()
    trades['date'] = pd.to_datetime(
        trades['date_time_trade'], errors='coerce', dayfirst=True).dt.date
    refdata = trades[['date']].drop_duplicates().sort_values(by='date').reset_index(drop=True).copy()
    trades['settle_date'] = calculate_value(raw_data, refdata)['settle_date']
    trades['trade_price'] = raw_data['Giá khớp\n(Matched Price)']
    trades['time_zone'] = "UTC -5"

    trades['taxes'] = raw_data.apply(calculate_Tax, axis=1)
    trades['commission'] = raw_data['EX_BID'] + raw_data['Tariff']
    trades['cost_basis'] = raw_data.apply(cosbasic, axis=1)
    trades['relatedtransactionid'] = ""
    trades['order_type'] = 'LMT'

    if df_old is None:
        result = process_trade(trades)
        try:
            pool = await db.connect_db(dbname, user, password, host, port)
            result_trades = db.transform_df(result)
            await db.insert_table_data(pool, 'trades', result_trades)
        except asyncpg.PostgresError as e:
            print("Error  {}".format(e))
            return None
    else:
        result = process_trade_load_n(trades, df_old)
        try:
            pool = await db.connect_db(dbname, user, password, host, port)
            result_trades = db.transform_df(result)
            await db.upsert_data_in_batches(pool, 'trades', result_trades,'self_generated_trade_id',batch_size=100)
        except asyncpg.PostgresError as e:
            print("Error  {}".format(e))
            return None
    return result


def calculate_BIDV(row):
        """Tính commission dựa trên tổng proceeds."""
        total_proceeds = -raw_data[raw_data['date'] == row['date']]['proceeds'].sum()
        if total_proceeds < 50000000:
            return  - (total_proceeds / total_proceeds * 50000) if total_proceeds > 0 else 0
        else:
            return row['proceeds'] * 0.001


def calculate_Tax(row):
    if ((row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row['side'] == "Long")):
        proceeds = row['Giá khớp\n(Matched Price)'] * row['KL khớp\n(Matched Quantity)']
        tax = -proceeds * 0.001  # 0.1% = 0.001
        return tax
    else:
        return 0
def NetCash(row):
    return row['Taxes'] + row['proceeds'] + row['commission']

def calculate_open_close(row):
        if ((row['Mua/Bán\n(Buy/Sell)'] == "Buy" and row.get('side', 'Long') == "Long") or
                (row['Mua/Bán\n(Buy/Sell)'] == "Div" and row.get('side', 'Long') == "Long") or
                (row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row.get('side', 'Long') == "Short")):
            return "O"
        elif ((row['Mua/Bán\n(Buy/Sell)'] == "Sell" and row.get('side', 'Long') == "Long") or
              (row['Mua/Bán\n(Buy/Sell)'] == "Buy" and row.get('side', 'Long') == "Short")):
            return "C"
        else:
            return ""

def process_data_securities_holding(trades):
    security_holding = pd.DataFrame()
    security_holding['transaction_id'] = trades['transaction_id']
    security_holding['open_close'] = trades['open_close']
    security_holding['side'] = 'Long'
    def calculate_cost_basis(row):
        if row['open_close'] == 'O':
            return row['cost_basis']/row['quantity']
        else:
            return ""
    def set_data_times_open(row):
        if row['open_close'] == 'O':
            return row['date_time_trade']
        else:
            return ""

    def calculate_quality(row, trades):
        if row['open_close'] == "C":
            return ""
        else:
            # Lọc các hàng trong df nơi cột AL khớp với BR + "C"
            filter_condition = (trades['transaction_id'] == row['transaction_id']) & (trades['open_close'] == 'C')
            total_sum = trades.loc[filter_condition, 'quantity'].sum() or 0  # Tính tổng cột AT cho các hàng khớp
            return row['quantity'] - total_sum
    def calculate_holding_period_date(row):
        if row['open_close'] == 'O':
            return row['open_date_time']
        else:
            return ""
    security_holding['cost_basis'] = trades.apply(calculate_cost_basis, axis=1)
    security_holding['open_date_time'] = trades.apply(set_data_times_open, axis=1)
    security_holding['quantity'] = trades.apply(lambda row: calculate_quality(row, trades), axis=1)
    security_holding['holding_period_date'] = security_holding.apply(calculate_holding_period_date, axis=1)

    return security_holding

def reprocessing_rawdata(raw_data: pd.DataFrame) -> pd.DataFrame:
    # Tiền xử lý raw data
    raw_data['date'] = pd.to_datetime(raw_data['Ngày giao dịch\n(Transaction Date)'],
                                      errors='coerce', dayfirst=True).dt.date
    raw_data['side'] = 'Long'
    raw_data['proceeds'] = -(raw_data['Giá khớp\n(Matched Price)'] * raw_data['KL khớp\n(Matched Quantity)'])
    raw_data['Tariff'] = -(-raw_data['proceeds'] * 0.0015)
    raw_data['EX_BID'] = raw_data.apply(calculate_BIDV, axis=1)
    raw_data['Taxes'] = -raw_data.apply(calculate_Tax, axis=1)
    raw_data['commission'] = raw_data['EX_BID'] + raw_data['Tariff']
    raw_data['NetCash'] = raw_data.apply(NetCash, axis=1)
    raw_data['open_close'] = raw_data.apply(calculate_open_close, axis=1)
    return raw_data

# Hàm main orchestrator
async def main(raw_data):
    pool = await db.connect_db(dbname,user,password,host,port)
    df = await db.load(pool,'trades')
    # Xử lý giao dịch và chèn/ upsert vào DB
    await pool.close()
    if df is None:
        trades = await process_data_trades(raw_data)
    else:
        trades = await process_data_trades(raw_data,df)
    if trades is not None:
        # Lưu file CSV của trades
        trades.to_csv('result/trades.csv', index=False)
        print("✅ Dữ liệu trades đã được lưu vào 'result/trades.csv'")
    else:
        print("❌ Xảy ra lỗi khi xử lý trades.")

    # Xử lý securities holding (hàm synchronous)
    securities_holding = process_data_securities_holding(trades)
    securities_holding.to_csv('result/securities_holding.csv', index=False)
    print("✅ Dữ liệu securities holding đã được lưu vào 'result/securities_holding.csv'")

    pd.set_option('display.max_rows', None)  # Hiển thị tất cả các hàng
    pd.set_option('display.max_columns', None)  # Hiển thị tất cả các cột
    pd.set_option('display.expand_frame_repr', False)  # Tránh xuống dòng các cột
    pd.set_option('display.float_format', '{:.0f}'.format)  # Định dạng số float không có dấu chấm thập phân nếu cần

    print(end='\n\n')

if __name__ == '__main__':
    # Đọc file raw data từ Excel
    file_path = os.getenv('path_rawdata')

    # raw_data = pd.read_excel(file_path,header= 7,sheet_name='I2-SSI')
    # raw_data = pd.read_excel(file_path, header=7, sheet_name='data_raw')
    raw_data = pd.read_excel(file_path,header= 7,sheet_name='test')
    raw_data = raw_data.iloc[:, 0:14]

    # Tiền xử lý raw data
    raw_data = reprocessing_rawdata(raw_data)
    asyncio.run(main(raw_data))
