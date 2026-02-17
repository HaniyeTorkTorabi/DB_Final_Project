import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR


def run_gold_etl():
    print("شروع عملیات ETL لایه Gold...")

    # 1. اتصال به دیتابیس
    try:
        engine = create_engine(CONNECTION_STR)
        print("اتصال به دیتابیس برقرار شد.")
    except Exception as e:
        print(f" خطا در اتصال: {e}")
        return

    # 2. EXTRACT: خواندن داده‌ها از لایه Silver
    print("در حال خواندن داده‌های تمیز شده از Silver...")
    try:
        query = "SELECT * FROM silver.cleaned_dataset"
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f" خطا در خواندن جدول سیلور: {e}")
        return

    # 3. TRANSFORM: پاکسازی و محاسبات

    # الف) حذف تکراری‌های کامل (Exact Duplicates)
    initial_count = len(df)
    df.drop_duplicates(inplace=True)
    final_count = len(df)
    if initial_count != final_count:
        print(f"تعداد {initial_count - final_count} رکورد کاملاً تکراری حذف شد.")

    # ب) استانداردسازی نام ستون‌ها (Snake Case)
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # ج) محاسبات بیزنس (Business Logic)
    print("در حال انجام محاسبات و مهندسی ویژگی‌ها...")

    # 1. محاسبه درآمد بر کیلومتر (Revenue Per KM)
    # مدیریت تقسیم بر صفر: اگر مسافت > 0 بود تقسیم کن، وگرنه 0 بگذار
    df["revenue_per_km"] = np.where(
        df["ride_distance"] > 0, df["booking_value"] / df["ride_distance"], 0
    ).round(2)

    # 2. دسته‌بندی مسافت (Distance Category)
    conditions = [
        (df["ride_distance"] <= 5),  # کوتاه
        (df["ride_distance"] > 5) & (df["ride_distance"] <= 15),  # متوسط
        (df["ride_distance"] > 15),  # طولانی
    ]
    choices = ["Short_Trip", "Medium_Trip", "Long_Trip"]

    # استفاده از np.select برای نگاشت شرط‌ها به مقادیر
    df["distance_category"] = np.select(conditions, choices, default="Medium_Trip")

    # 3. شناسایی سفرهای با ارزش بالا (High Value Rides)
    # محاسبه میانگین قیمت کل شبکه
    avg_price = df["booking_value"].mean()
    print(f"   ℹ️ میانگین قیمت سفرها: {avg_price:.2f}")

    # اگر قیمت بیشتر از میانگین بود 1، وگرنه 0
    df["is_high_value"] = np.where(df["booking_value"] > avg_price, 1, 0)

    # د) ایجاد کلید جایگزین (Surrogate Key)
    print("در حال ایجاد کلید اصلی (gold_record_id)...")

    # ایجاد یک سریال عددی از 1 تا N
    df["gold_record_id"] = range(1, len(df) + 1)

    # انتقال gold_record_id به ستون اول جدول
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("gold_record_id")))
    df = df[cols]

    # 4. LOAD: بارگذاری و اعمال محدودیت‌ها
    print("در حال بارگذاری داده‌ها در لایه Gold...")

    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
            conn.execute(text("DROP TABLE IF EXISTS gold.dataset CASCADE;"))
            conn.commit()

        # ذخیره دیتافریم در دیتابیس
        # index=False چون خودمان gold_record_id ساختیم
        df.to_sql("dataset", engine, schema="gold", if_exists="replace", index=False)

        print("در حال اعمال محدودیت‌های امنیتی (Constraints)...")
        with engine.connect() as conn:
            # 1. تعریف Primary Key
            conn.execute(
                text(
                    """
                ALTER TABLE gold.dataset 
                ADD CONSTRAINT pk_gold_record_id PRIMARY KEY (gold_record_id);
            """
                )
            )

            # 2. محدودیت امتیاز راننده (بین 0 تا 5)
            conn.execute(
                text(
                    """
                ALTER TABLE gold.dataset 
                ADD CONSTRAINT check_driver_rating 
                CHECK (driver_ratings IS NULL OR (driver_ratings >= 0 AND driver_ratings <= 5));
            """
                )
            )

            # 3. محدودیت امتیاز مشتری (بین 0 تا 5)
            conn.execute(
                text(
                    """
                ALTER TABLE gold.dataset 
                ADD CONSTRAINT check_customer_rating 
                CHECK (customer_rating IS NULL OR (customer_rating >= 0 AND customer_rating <= 5));
            """
                )
            )

            conn.commit()

        print(f"عملیات با موفقیت انجام شد!")
        print(f"جدول gold.dataset با {len(df)} رکورد و ستون‌های محاسباتی ایجاد گردید.")

    except Exception as e:
        print(f"خطا در مرحله بارگذاری یا اعمال محدودیت‌ها: {e}")


if __name__ == "__main__":
    run_gold_etl()
