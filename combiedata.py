import numpy as np
import pandas as pd
import os

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


