import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR


def get_season(month):
    """تبدیل ماه میلادی به فصل"""
    if month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Autumn"
    else:
        return "Winter"


def run_silver_etl():
    print("شروع عملیات ETL لایه Silver (نسخه نهایی و اصلاح شده)...")

    # 1. اتصال به دیتابیس
    try:
        engine = create_engine(CONNECTION_STR)
        print("✅ اتصال به دیتابیس برقرار شد.")
    except Exception as e:
        print(f"❌ خطا در اتصال: {e}")
        return

    # 2. Extract: خواندن داده‌های خام از لایه Bronze
    print("در حال خواندن داده‌های خام از Bronze...")
    try:
        query = "SELECT * FROM bronze.raw_dataset"
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"خطا در خواندن جدول برنز: {e}")
        return

    print("اقدام ۰: حذف کاراکترهای اضافی (Quotes) از شناسه‌ها...")

    id_cols = ["Booking_ID", "Customer_ID"]
    for col in id_cols:
        # 1. تبدیل به رشته
        # 2. حذف تمام کاراکترهای " (چه یکی چه سه تا)
        # 3. حذف فاصله‌های خالی ابتدا و انتها
        df[col] = df[col].astype(str).str.replace('"', '', regex=False).str.strip()

    print(f"   ✅ نمونه اصلاح شده: {df['Booking_ID'].iloc[0]}")
    # ==========================================================

    # 3. Transform & Feature Engineering

    # --- اقدام ۱: مهندسی زمان ---
    print("اقدام ۱: مهندسی ویژگی‌های زمانی (Time & Season)...")
    # ترکیب تاریخ و ساعت برای ساخت Timestamp
    df["Timestamp"] = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")

    df["Month"] = df["Timestamp"].dt.month
    df["Day"] = df["Timestamp"].dt.day
    df["Hour"] = df["Timestamp"].dt.hour
    df["Day_Name"] = df["Timestamp"].dt.day_name()
    # تشخیص روزهای آخر هفته (شنبه و یکشنبه برای دیتای خارجی)
    df["Is_Weekend"] = np.where(df["Timestamp"].dt.dayofweek >= 5, 1, 0)
    df["Season"] = df["Month"].apply(get_season)

    # دسته‌بندی ساعات روز
    bins = [0, 5, 12, 17, 21, 24]
    labels = ["Night", "Morning", "Afternoon", "Evening", "Night"]
    df["Time_Category"] = pd.cut(
        df["Hour"],
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
        ordered=False,
    )

    # --- اقدام ۲: یکپارچه‌سازی هوشمند دلایل لغو ---
    print("اقدام ۲: یکپارچه‌سازی دلایل لغو با برچسب منشأ...")

    def get_smart_cancellation_reason(row):
        status = row["Booking_Status"]
        if status == "Cancelled by Customer":
            return f"Customer: {row['Reason_for_cancelling_by_Customer']}"
        elif status == "Cancelled by Driver":
            return f"Driver: {row['Driver_Cancellation_Reason']}"
        elif status == "Incomplete":
            return f"Incomplete: {row['Incomplete_Rides_Reason']}"
        elif status == "No Driver Found":
            return "System: No Driver Found"
        else:
            return None  # برای سفرهای تکمیل شده

    df["Unified_cancellation_reason"] = df.apply(get_smart_cancellation_reason, axis=1)

    # حذف ستون‌های اضافی که دیگر نیاز نداریم
    cols_to_drop = [
        "Date", "Time",
        "Reason_for_cancelling_by_Customer",
        "Driver_Cancellation_Reason",
        "Incomplete_Rides_Reason",
        "Cancelled_Rides_by_Customer",
        "Cancelled_Rides_by_Driver",
        "Incomplete_Rides",
    ]
    df.drop(columns=cols_to_drop, inplace=True)

    # --- اقدام ۳: مدیریت Null و فلگ‌ها ---
    print("اقدام ۳: مدیریت Null و ایجاد متادیتا...")

    # ساخت فلگ قبل از پر کردن نال‌ها
    df["Has_Driver_Rating"] = df["Driver_Ratings"].notnull().astype(int)
    df["Has_Customer_Rating"] = df["Customer_Rating"].notnull().astype(int)

    # پر کردن مقادیر مالی با صفر (چون سفر انجام نشده درآمد ندارد)
    df["Booking_Value"] = df["Booking_Value"].fillna(0)
    df["Ride_Distance"] = df["Ride_Distance"].fillna(0)

    # --- اقدام ۴: پر کردن امتیازها (فقط برای سفرهای موفق) ---
    print("اقدام ۴: پر کردن امتیازهای گمشده در سفرهای موفق...")

    mask_completed = df["Booking_Status"] == "Completed"

    # محاسبه میانه امتیازها (فقط از سفرهای تکمیل شده)
    median_driver = df.loc[mask_completed, "Driver_Ratings"].median()
    median_cust = df.loc[mask_completed, "Customer_Rating"].median()

    # پر کردن نال‌ها با میانه
    df.loc[mask_completed, "Driver_Ratings"] = df.loc[mask_completed, "Driver_Ratings"].fillna(median_driver)
    df.loc[mask_completed, "Customer_Rating"] = df.loc[mask_completed, "Customer_Rating"].fillna(median_cust)

    # گزارش آماری کوتاه
    print("\n📊 --- گزارش آماری لایه Silver ---")
    print(f"تعداد کل رکوردها: {len(df)}")
    print(f"بازه زمانی: {df['Timestamp'].min()} تا {df['Timestamp'].max()}")
    print(f"تعداد ستون‌ها: {df.shape[1]}")

    # 4. Load: ذخیره در دیتابیس
    print("\n📤 در حال ذخیره در لایه Silver...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.commit()

    df.to_sql(
        "cleaned_dataset", engine, schema="silver", if_exists="replace", index=False
    )
    print(f"پایان عملیات. جدول silver.cleaned_dataset با موفقیت و شناسه‌های تمیز ذخیره شد.")


if __name__ == "__main__":
    run_silver_etl()