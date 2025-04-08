import uuid
import time

import asyncio
import asyncpg

import os
import dotenv
import numpy as np
import pandas as pd

dotenv.load_dotenv()

dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

async def connect_db(database, user, password, host="localhost", port="5432"):
    try:
        pool = await asyncpg.create_pool(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            min_size=1,
            max_size=20
        )
        print(f"✅ Successfully connected to {dbname} at {host}:{port}")
        return pool
    except asyncpg.PostgresError as e:
        print(f"❌ Error connecting to database: {e}")
        return None


async def create_table_if_not_exists(pool: asyncpg.pool.Pool, table_name, df):
    """Create table if it doesn't exist based on DataFrame structure"""

    df.columns = df.columns.str.lower()  # Chuyển tất cả tên cột về chữ thường
    # Xác định kiểu dữ liệu PostgreSQL dựa trên pandas dtypes
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        if col == 'time_zone':
            pg_type = 'TEXT'
        elif col == 'transaction_id':
            pg_type = 'TEXT'
        elif col == 'date_time_trade':
            pg_type = 'TEXT'
        elif col == 'date':
            pg_type = 'TEXT'
        elif col == 'settle_date':
            pg_type = 'TEXT'
        elif pd.api.types.is_integer_dtype(dtype):
            # Kiểm tra nếu giá trị tối đa vượt quá giới hạn int32
            if df[col].max() > 2147483647:
                pg_type = "BIGINT"
            else:
                pg_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            pg_type = "NUMERIC"
        elif pd.api.types.is_bool_dtype(dtype):
            pg_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype) or "date" in col or "time" in col:
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"

        columns.append(f'"{col}" {pg_type}')

    # Set primary key cho cột đầu tiên
    if columns:
        columns[0] = columns[0] + " PRIMARY KEY"

    # Tạo câu lệnh SQL
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

    async with pool.acquire() as conn:
        try:
            await conn.execute(create_table_query)
            print(f"✅ Table '{table_name}' created successfully")
        except asyncpg.PostgresError as e:
            print(f"❌ Error creating table '{table_name}': {e}")


async def insert_data_in_batches(pool: asyncpg.pool.Pool, table_name, df, batch_size=10000):
    """Chia dữ liệu thành batch, dùng Connection Pool và insert song song vào PostgreSQL"""
    # Kết nối đến PostgreSQL bằng Connection Pool

    # Chuẩn bị cột và query INSERT động
    columns = list(df.columns)
    columns_str = ", ".join(columns)
    values_placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))  # asyncpg dùng $1, $2...
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders})"

    # Chuyển DataFrame thành danh sách tuple
    data_tuples = [
        tuple(x if pd.notna(x) else None for x in row)
        for row in df.to_numpy()
    ]



    async with pool:  # Tự động đóng pool sau khi hoàn thành
        # Chia dữ liệu thành nhiều batch nhỏ
        batches = np.array_split(data_tuples, len(data_tuples) // batch_size + 1)

        # Chèn dữ liệu theo từng batch
        async def insert_batch(batch):
            async with pool.acquire() as conn:  # Mượn kết nối từ pool
                try:
                    await conn.executemany(query, batch)
                    print(f"✅ Inserted {len(batch)} rows")
                except asyncpg.PostgresError as e:
                    print(f"❌ Error inserting batch: {e}")

        # Tạo danh sách task insert song song

        tasks = [insert_batch(batch) for batch in batches]
        start_time = time.time()
        # Chạy tất cả các task song song
        await asyncio.gather(*tasks)
        end_time = time.time()

    print(
        f"🎯 Done! Inserted {len(data_tuples)} rows in {end_time - start_time:.2f} seconds using batch size {batch_size}")


async def load_data_from_db(pool: asyncpg.pool.Pool, table_name) -> pd.DataFrame:
    """
    Load dữ liệu từ bảng trong PostgreSQL vào một pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name};"
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    # Chuyển danh sách các asyncpg Record thành danh sách dict
    data = [dict(row) for row in rows]

    # Tạo DataFrame từ danh sách dict
    df_loaded = pd.DataFrame(data)

    return df_loaded if len(df_loaded) > 0 else None


async def upsert_data_in_batches(pool: asyncpg.pool.Pool, table_name, df, conflict_target, batch_size=10000):
    """
    Chia dữ liệu thành batch và thực hiện upsert (INSERT ... ON CONFLICT DO UPDATE) vào PostgreSQL.
    """
    # Chuẩn bị danh sách cột và các placeholder
    columns = list(df.columns)

    columns_str = ", ".join(f'"{col}"' for col in columns)
    values_placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

    # Danh sách các cột sẽ cập nhật, loại trừ conflict_target (và có thể loại bỏ một số cột nếu cần)
    update_columns = [col for col in columns if col not in {conflict_target, 'open_close','quantity','relatedtransactionid','dim_security_id','settle_date'}]
    update_assignments = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)

    # Câu lệnh upsert, cập nhật luôn updated_at = now()
    query = f"""
    INSERT INTO {table_name} ({columns_str})
    VALUES ({values_placeholders})
    ON CONFLICT ("{conflict_target}") DO UPDATE SET
      {update_assignments};
    """

    # Chuyển DataFrame thành danh sách tuple, thay thế NaN bằng None
    data_tuples = [
        tuple(x if pd.notna(x) else None for x in row)
        for row in df.to_numpy()
    ]

    async with pool:  # Sử dụng pool và tự động đóng sau khi hoàn thành
        # Chia dữ liệu thành nhiều batch
        batches = np.array_split(data_tuples, len(data_tuples) // batch_size + 1)

        async def upsert_batch(batch):
            async with pool.acquire() as conn:
                try:
                    await conn.executemany(query, batch)
                    print(f"✅ Upserted {len(batch)} rows")
                except asyncpg.PostgresError as e:
                    print(f"❌ Error upserting batch: {e}")

        tasks = [upsert_batch(batch) for batch in batches]
        start_time = time.time()
        await asyncio.gather(*tasks)
        end_time = time.time()

    print(
        f"🎯 Done! Upserted {len(data_tuples)} rows in {end_time - start_time:.2f} seconds using batch size {batch_size}"
    )

async def insert_table_data(pool: asyncpg.pool.Pool, table_name: str, dataframe: pd.DataFrame):

    # Kiểm tra kết nối
    try:
        async with pool.acquire() as conn:
            print("database is already")
    except asyncpg.PostgresError as e:
        print(f"error: {e}")

    # Tạo bảng nếu chưa tồn tại
    await create_table_if_not_exists(pool, table_name, dataframe)

    # Chèn dữ liệu theo batch
    await insert_data_in_batches(
        pool= pool,
        table_name='trades',
        df=dataframe,
        batch_size=100
    )
    await pool.close()


async def load(pool : asyncpg.pool.Pool,table_name: str) -> pd.DataFrame | None:
    try:
        df_from_db = await load_data_from_db(pool, table_name)
        print("Dữ liệu được load từ database:")
        await pool.close()
        return df_from_db
    except asyncpg.PostgresError as e:
        print(f"❌ Error: {e}")
        return None

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    df.insert(0, 'self_generated_trade_id', [str(uuid.uuid4()) for _ in range(len(df))])
    df.insert(1, 'dim_security_id', [np.nan for value in range(len(df))])
    df['transaction_id'] = df['transaction_id'].astype(str)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')  # Chuyển thành kiểu số
    df['remain'] = pd.to_numeric(df['remain'], errors='coerce')  # Chuyển thành kiểu số
    df['trade_price'] = pd.to_numeric(df['trade_price'], errors='coerce')  # Chuyển thành kiểu số
    df['taxes'] = pd.to_numeric(df['taxes'], errors='coerce')  # Chuyển thành kiểu số
    df['commission'] = pd.to_numeric(df['commission'], errors='coerce')  # Chuyển thành kiểu số
    df['cost_basis'] = pd.to_numeric(df['cost_basis'], errors='coerce')  # Chuyển thành kiểu số
    df['relatedtransactionid'] = pd.to_numeric(df['relatedtransactionid'], errors='coerce')  # Chuyển thành kiểu số
    df['remain'] = pd.to_numeric(df['remain'], errors='coerce')  # Chuyển thành kiểu số
    df['settle_date'] = df['settle_date'].apply(
        lambda d: d.strftime("%Y-%m-%d") if pd.notnull(d) and not isinstance(d, str) else (d if pd.notnull(d) else None)
    )
    df['date'] = df['date'].apply(
        lambda d: d.strftime("%Y-%m-%d") if pd.notnull(d) and not isinstance(d, str) else (d if pd.notnull(d) else None)
    )
    df = df.where(pd.notnull(df), None)
    return df
# df = pd.read_csv('./result/trades.csv').drop(columns=['date'])


# columns = df.columns.tolist()
# data = df.loc[:].values.tolist()
# df.insert(0, 'self_generated_trade_id', [str(uuid.uuid4()) for _ in range(len(df))])
# df.insert(1,'dim_security_id', [np.nan for value in range(len(df))])
# df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')  # Chuyển thành kiểu số
# df['remain'] = pd.to_numeric(df['remain'], errors='coerce')  # Chuyển thành kiểu số
# df['trade_price'] = pd.to_numeric(df['trade_price'], errors='coerce')  # Chuyển thành kiểu số
# df['taxes'] = pd.to_numeric(df['taxes'], errors='coerce')  # Chuyển thành kiểu số
# df['commission'] = pd.to_numeric(df['commission'], errors='coerce')  # Chuyển thành kiểu số
# df['cost_basis'] = pd.to_numeric(df['cost_basis'], errors='coerce')  # Chuyển thành kiểu số
# df['relatedTransactionID'] = pd.to_numeric(df['relatedTransactionID'], errors='coerce')  # Chuyển thành kiểu số
# df['remain'] = pd.to_numeric(df['remain'], errors='coerce')  # Chuyển thành kiểu số


