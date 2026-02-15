import pandas as pd
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR  # تنظیمات اتصال از فایل کانفیگ


def run_bronze_etl():
    print("🚀 شروع عملیات ETL لایه Bronze...")

    # 1. اتصال به دیتابیس
    try:
        engine = create_engine(CONNECTION_STR)
        conn = engine.connect()
        print("✅ اتصال به دیتابیس برقرار شد.")
    except Exception as e:
        print(f"❌ خطا در اتصال به دیتابیس: {e}")
        print("💡 نکته: آیا دستور pip install psycopg2-binary را اجرا کردید؟")
        return

    # 2. خواندن فایل CSV و استانداردسازی نام ستون‌ها
    try:
        csv_file_path = "Database.csv"
        print(f"📂 در حال خواندن فایل {csv_file_path} ...")
        df = pd.read_csv(csv_file_path)

        # تبدیل نام ستون‌ها: فاصله را با _ عوض می‌کنیم (مثلاً "Booking ID" می‌شود "Booking_ID")
        df.columns = df.columns.str.replace(" ", "_").str.replace("/", "_")
        print(f"   نام ستون‌ها برای دیتابیس استاندارد شد: {list(df.columns[:3])} ...")

    except Exception as e:
        print(f"❌ خطا در خواندن فایل CSV: {e}")
        return

    # 3. ساخت Schema و Table (دقیقاً طبق خواسته پروژه)
    try:
        # ایجاد اسکیما bronze
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

        # حذف جدول قدیمی برای اجرای دوباره
        conn.execute(text("DROP TABLE IF EXISTS bronze.raw_dataset;"))

        create_table_query = """
        CREATE TABLE bronze.raw_dataset (
            "Date" TEXT,
            "Time" TEXT,
            "Booking_ID" TEXT,
            "Booking_Status" TEXT,
            "Customer_ID" TEXT,
            "Vehicle_Type" TEXT,
            "Cancelled_Rides_by_Customer" FLOAT,
            "Reason_for_cancelling_by_Customer" TEXT,
            "Cancelled_Rides_by_Driver" FLOAT,
            "Driver_Cancellation_Reason" TEXT,
            "Incomplete_Rides" FLOAT,
            "Incomplete_Rides_Reason" TEXT,
            "Booking_Value" FLOAT,
            "Ride_Distance" FLOAT,
            "Driver_Ratings" FLOAT,
            "Customer_Rating" FLOAT,
            "Payment_Method" TEXT
        );
        """
        conn.execute(text(create_table_query))
        conn.commit()
        print("✅ جدول bronze.raw_dataset دقیقاً طبق فایل تمرین ساخته شد.")

        # 4. بارگذاری داده‌ها
        print("در حال انتقال داده‌ها به دیتابیس...")
        df.to_sql(
            "raw_dataset", engine, schema="bronze", if_exists="append", index=False
        )
        print(f"تمام {len(df)} رکورد با موفقیت در جدول bronze.raw_dataset ذخیره شد.")

    except Exception as e:
        print(f"❌ خطا در عملیات دیتابیس: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_bronze_etl()
