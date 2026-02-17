import time
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR

# 1. تنظیمات اتصال
engine = create_engine(CONNECTION_STR)

# 2. تعریف کوئری سنگین (Heavy Query)
# سناریو: گزارش تعداد سفر و میانگین درآمد برای نوع خاصی از خودرو در 6 ماه اول سال
HEAVY_QUERY = """
SELECT 
    unified_cancellation_reason,
    COUNT(*) as total_rides,
    AVG(booking_value) as avg_income,
    SUM(ride_distance) as total_distance
FROM gold.dataset
WHERE 
    vehicle_type = 'Premier Sedan' 
    AND timestamp >= '2024-01-01 00:00:00' 
    AND timestamp <= '2024-06-30 23:59:59'
GROUP BY 
    unified_cancellation_reason
ORDER BY 
    total_rides DESC;
"""

# 3. تعریف دستورات ایندکس
INDEX_NAME = "idx_vehicle_timestamp"
# استفاده از Composite Index (اول ستون تساوی، دوم ستون بازه)
CREATE_INDEX_SQL = f"CREATE INDEX {INDEX_NAME} ON gold.dataset (vehicle_type, timestamp);"
DROP_INDEX_SQL = f"DROP INDEX IF EXISTS {INDEX_NAME};"


def run_benchmark(label, connection):
    print(f"\n{'=' * 50}")
    print(f"📡 RUNNING BENCHMARK: {label}")
    print(f"{'=' * 50}")

    start_time = time.time()

    # اجرای EXPLAIN ANALYZE برای دیدن پلن واقعی
    result = connection.execute(text(f"EXPLAIN ANALYZE {HEAVY_QUERY}"))
    rows = result.fetchall()

    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000

    print(f"⏱️  Execution Time: {duration_ms:.2f} ms")
    print("-" * 20)
    print("📄 Execution Plan:")
    for row in rows[:50]:
        print(f"   {row[0]}")

    return duration_ms


def main():
    try:
        with engine.connect() as conn:
            # گام صفر: پاکسازی محیط (حذف ایندکس قبلی احتمالی)
            conn.execute(text(DROP_INDEX_SQL))
            conn.commit()

            # گام اول: تست بدون ایندکس
            time_before = run_benchmark("WITHOUT INDEX (Full Scan)", conn)

            # گام دوم: ساخت ایندکس
            print("\n🔨 Creating Index... Please wait.")
            start_create = time.time()
            conn.execute(text(CREATE_INDEX_SQL))
            conn.commit()
            print(f"✅ Index '{INDEX_NAME}' created in {(time.time() - start_create):.2f} seconds.")

            # گام سوم: تست با ایندکس
            time_after = run_benchmark("WITH INDEX (Index Scan)", conn)

            # گام چهارم: محاسبه درصد بهبود
            if time_before > 0:
                improvement = ((time_before - time_after) / time_before) * 100
                print(f"\n{'=' * 50}")
                print(f"🚀 FINAL RESULT:")
                print(f"Before Indexing: {time_before:.2f} ms")
                print(f"After Indexing:  {time_after:.2f} ms")
                print(f"Performance Boost: {improvement:.1f}% FASTER")
                print(f"{'=' * 50}\n")


    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()