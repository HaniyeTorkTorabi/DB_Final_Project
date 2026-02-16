from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR
from typing import List, Optional
import datetime

app = FastAPI(title="Uber Analytics API", version="6.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = create_engine(CONNECTION_STR)

# ==========================================
# 🛠️ تابع کمکی برای ساخت فیلترهای پویا
# ==========================================
def build_filter_clause(start_date: str, end_date: str, vehicles: List[str]):
    conditions = []
    params = {}

    # 1. فیلتر زمانی
    if start_date:
        conditions.append("timestamp >= :start_date")
        params["start_date"] = start_date + " 00:00:00"
    if end_date:
        conditions.append("timestamp <= :end_date")
        params["end_date"] = end_date + " 23:59:59"

    # 2. فیلتر نوع خودرو
    if vehicles:
        conditions.append("vehicle_type IN :vehicles")
        params["vehicles"] = tuple(vehicles)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, params


# ==========================================
# 1. KPI Endpoint
# ==========================================
@app.get("/analytics/kpi")
def get_kpis(start_date: Optional[str] = None, end_date: Optional[str] = None, vehicles: List[str] = Query(None)):
    try:
        where_clause, params = build_filter_clause(start_date, end_date, vehicles)
        with engine.connect() as conn:
            # الف) کل سفرها
            q1 = text(f"SELECT COUNT(*) FROM gold.dataset WHERE {where_clause}")
            total = conn.execute(q1, params).scalar() or 0

            # ب) سفرهای موفق
            q2 = text(f"SELECT COUNT(*) FROM gold.dataset WHERE booking_status = 'Completed' AND {where_clause}")
            success = conn.execute(q2, params).scalar() or 0

            # ج) درآمد
            q3 = text(f"SELECT SUM(booking_value) FROM gold.dataset WHERE booking_status = 'Completed' AND {where_clause}")
            rev = conn.execute(q3, params).scalar() or 0

            rate = (success / total * 100) if total > 0 else 0

            return {"total_bookings": total, "successful_bookings": success, "total_revenue": int(rev), "success_rate": round(rate, 2)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 2. Pie Charts Endpoint (اصلاح شده برای دلایل دقیق لغو)
# ==========================================
@app.get("/analytics/pie-data")
def get_pie_data(start_date: Optional[str] = None, end_date: Optional[str] = None, vehicles: List[str] = Query(None)):
    try:
        where_clause, params = build_filter_clause(start_date, end_date, vehicles)
        with engine.connect() as conn:
            # ✅ تغییر اصلی اینجاست: استفاده از unified_cancellation_reason
            # شرط IS NOT NULL یعنی فقط اونایی که دلیل لغو دارن رو بیار
            q_cancel = text(f"""
                SELECT unified_cancellation_reason, COUNT(*) as count 
                FROM gold.dataset 
                WHERE unified_cancellation_reason IS NOT NULL
                AND {where_clause}
                GROUP BY unified_cancellation_reason
            """)

            # روش پرداخت
            q_pay = text(f"""
                SELECT payment_method, COUNT(*) as count 
                FROM gold.dataset 
                WHERE booking_status = 'Completed' AND {where_clause}
                GROUP BY payment_method
            """)

            return {
                "cancellations": [dict(r._mapping) for r in conn.execute(q_cancel, params)],
                "payments": [dict(r._mapping) for r in conn.execute(q_pay, params)]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 3. Bar Charts Endpoint
# ==========================================
@app.get("/analytics/bar-data")
def get_bar_data(start_date: Optional[str] = None, end_date: Optional[str] = None, vehicles: List[str] = Query(None)):
    try:
        where_clause, params = build_filter_clause(start_date, end_date, vehicles)
        with engine.connect() as conn:
            q = text(f"""
                SELECT vehicle_type, COUNT(*) as trip_count, 
                    AVG(driver_ratings) as avg_driver,
                    AVG(customer_rating) as avg_customer
                FROM gold.dataset 
                WHERE booking_status = 'Completed' AND {where_clause}
                GROUP BY vehicle_type
                ORDER BY trip_count DESC
            """)
            return [dict(r._mapping) for r in conn.execute(q, params)]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 4. Line Charts Endpoint
# ==========================================
@app.get("/analytics/line-data")
def get_line_data(start_date: Optional[str] = None, end_date: Optional[str] = None, vehicles: List[str] = Query(None)):
    try:
        where_clause, params = build_filter_clause(start_date, end_date, vehicles)
        with engine.connect() as conn:
            q_hour = text(f"SELECT hour, COUNT(*) as count FROM gold.dataset WHERE {where_clause} GROUP BY hour ORDER BY hour")
            q_day = text(f"SELECT day_name, COUNT(*) as count FROM gold.dataset WHERE {where_clause} GROUP BY day_name")
            return {"hourly": [dict(r._mapping) for r in conn.execute(q_hour, params)], "daily": [dict(r._mapping) for r in conn.execute(q_day, params)]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))