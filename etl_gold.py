import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR


def run_gold_etl():
    print("شروع عملیات ETL لایه Gold (نسخه نهایی - کلید جایگزین)...")

    engine = create_engine(CONNECTION_STR)

    # 1. Extract
    print("خواندن داده‌ها از Silver...")
    query = 'SELECT * FROM silver.cleaned_dataset'
    df = pd.read_sql(query, engine)

    # --- استراتژی داده‌های تکراری ---
    print("فقط حذف ردیف‌های کاملاً تکراری (Exact Duplicates)...")
    initial_count = len(df)
    df.drop_duplicates(inplace=True)
    final_count = len(df)
    print(f"تعداد {initial_count - final_count} رکورد کاملاً تکراری حذف شد.")

    # 2. Transform (محاسبات و استانداردسازی)
    print("استانداردسازی و محاسبات...")

    # تغییر نام ستون‌ها به snake_case
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # محاسبه Revenue Per KM
    df['revenue_per_km'] = np.where(
        df['ride_distance'] > 0,
        df['booking_value'] / df['ride_distance'],
        0
    ).round(2)

    # دسته‌بندی مسافت
    conditions = [
        (df['ride_distance'] <= 5),
        (df['ride_distance'] > 5) & (df['ride_distance'] <= 15),
        (df['ride_distance'] > 15)
    ]
    choices = ['Short_Trip', 'Medium_Trip', 'Long_Trip']
    df['distance_category'] = np.select(conditions, choices, default='Medium_Trip')

    # دسته‌بندی ارزش سفر
    avg_price = df['booking_value'].mean()
    df['is_high_value'] = np.where(df['booking_value'] > avg_price, 1, 0)

    # --- ایجاد کلید جایگزین (Surrogate Key) و انتقال به ستون اول ---
    print("در حال ایجاد کلید جایگزین و تنظیم ترتیب ستون‌ها...")

    # 1. ساخت ستون ID
    df['gold_record_id'] = range(1, len(df) + 1)

    # 2. انتقال به ستون اول (Reordering Columns)
    # لیست تمام ستون‌ها را می‌گیریم
    cols = df.columns.tolist()
    # gold_record_id را از لیست حذف می‌کنیم و به اول لیست اضافه می‌کنیم
    cols.insert(0, cols.pop(cols.index('gold_record_id')))
    # ترتیب جدید را به DataFrame اعمال می‌کنیم
    df = df[cols]

    # 3. Load & Constraints
    print("آماده‌سازی دیتابیس...")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.execute(text("DROP TABLE IF EXISTS gold.dataset CASCADE;"))
        conn.commit()

    print("بارگذاری داده‌ها...")
    df.to_sql('dataset', engine, schema='gold', if_exists='replace', index=False)

    print("اعمال محدودیت‌های دیتابیس...")

    with engine.connect() as conn:
        # کلید اصلی روی gold_record_id (که الان ستون اول است)
        conn.execute(text("""
            ALTER TABLE gold.dataset 
            ADD CONSTRAINT pk_gold_record_id PRIMARY KEY (gold_record_id);
        """))

        # Check Constraints
        conn.execute(text("""
            ALTER TABLE gold.dataset 
            ADD CONSTRAINT check_driver_rating 
            CHECK (driver_ratings IS NULL OR (driver_ratings >= 0 AND driver_ratings <= 5));
        """))

        conn.execute(text("""
            ALTER TABLE gold.dataset 
            ADD CONSTRAINT check_customer_rating 
            CHECK (customer_rating IS NULL OR (customer_rating >= 0 AND customer_rating <= 5));
        """))

        conn.commit()

    print(f"عملیات موفقیت‌آمیز بود! جدول نهایی با {len(df)} رکورد ایجاد شد.")

if __name__ == "__main__":
    run_gold_etl()