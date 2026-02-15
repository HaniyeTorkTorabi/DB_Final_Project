from fastapi import FastAPI, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from db_config import CONNECTION_STR
from typing import Optional
from enum import Enum
import random
import datetime

# ==========================================
# 1. تنظیمات اپلیکیشن
# ==========================================
app = FastAPI(
    title="Uber Data Engineering API",
    description="Full CRUD with Automatic Data Enrichment",
    version="5.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = create_engine(CONNECTION_STR)


# ==========================================
# 2. مدل‌ها و توابع کمکی
# ==========================================

class VehicleType(str, Enum):
    Auto = "Auto"
    Premier_Sedan = "Premier Sedan"
    Go_Sedan = "Go Sedan"
    eBike = "eBike"
    Bike = "Bike"
    Go_Mini = "Go Mini"


class PaymentMethod(str, Enum):
    Cash = "Cash"
    UPI = "UPI"
    Card = "Card"
    Wallet = "Wallet"
    Debit_Card = "Debit Card"
    Credit_Card = "Credit Card"


class RideStatus(str, Enum):
    Completed = "Completed"
    Cancelled_Customer = "Cancelled by Customer"
    Cancelled_Driver = "Cancelled by Driver"
    Incomplete = "Incomplete"
    No_Driver = "No Driver Found"


PRICING_RATES = {"Bike": 1500, "eBike": 2000, "Auto": 3000, "Go Mini": 4000, "Go Sedan": 5500, "Premier Sedan": 8000}
BASE_FARE = 1000


def get_season_name(month):
    if month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Autumn"
    else:
        return "Winter"


class RideCreateSchema(BaseModel):
    date: str = Field(..., pattern=r"^\d{1,2}/\d{1,2}/\d{4}$", description="MM/DD/YYYY")
    time: str = Field(..., pattern=r"^\d{1,2}:\d{1,2}:\d{1,2}$", description="HH:MM:SS")
    customer_id: str = Field(..., pattern=r"^CID\d{7}$")
    vehicle_type: VehicleType
    payment_method: PaymentMethod
    driver_ratings: float = Field(default=5.0, ge=0, le=5)


class StatusUpdateSchema(BaseModel):
    status: RideStatus


# ==========================================
# 3. اندپوینت‌ها (CRUD)
# ==========================================

# ✅ READ (لیست و فیلتر)
@app.get("/rides/")
def read_rides(customer_id: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=1000)):
    query_str = "SELECT * FROM gold.dataset"
    params = {"limit": limit}
    if customer_id:
        query_str += " WHERE customer_id = :cid"
        params["cid"] = customer_id.strip().replace('"', '')
    query_str += " ORDER BY gold_record_id DESC LIMIT :limit"

    with engine.connect() as conn:
        result = conn.execute(text(query_str), params)
        return [dict(row._mapping) for row in result]


# ✅ CREATE (ثبت و غنی‌سازی خودکار ستون‌ها)
@app.post("/rides/", status_code=status.HTTP_201_CREATED)
def create_ride(ride: RideCreateSchema):
    try:
        # ۱. استخراج ویژگی‌های زمانی
        dt_obj = datetime.datetime.strptime(f"{ride.date} {ride.time}", "%m/%d/%Y %H:%M:%S")
        final_ts = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        month, hour, day_name = dt_obj.month, dt_obj.hour, dt_obj.strftime("%A")
        is_weekend = 1 if dt_obj.weekday() >= 5 else 0
        season = get_season_name(month)

        if 5 <= hour < 12:
            t_cat = "Morning"
        elif 12 <= hour < 17:
            t_cat = "Afternoon"
        elif 17 <= hour < 21:
            t_cat = "Evening"
        else:
            t_cat = "Night"

        # ۲. محاسبات مالی و مسافت
        sim_dist = round(random.uniform(2.0, 40.0), 2)
        calc_val = int(BASE_FARE + (sim_dist * PRICING_RATES.get(ride.vehicle_type.value, 3000)))
        rev_per_km = round(calc_val / sim_dist, 2)
        dist_cat = "Short_Trip" if sim_dist <= 5 else "Medium_Trip" if sim_dist <= 15 else "Long_Trip"

        with engine.connect() as conn:
            # ۳. تولید فیلدهای سیستمی (ID یکتا)
            while True:
                bid = f"CNR{random.randint(1000000, 9999999)}"
                if not conn.execute(text("SELECT 1 FROM gold.dataset WHERE booking_id = :b"), {"b": bid}).scalar():
                    break

            avg_p = conn.execute(text("SELECT AVG(booking_value) FROM gold.dataset")).scalar() or 0
            new_gid = (conn.execute(text("SELECT MAX(gold_record_id) FROM gold.dataset")).scalar() or 0) + 1

            # ۴. درج تمام ۲۲ ستون به صورت خودکار
            conn.execute(text("""
                INSERT INTO gold.dataset (
                    gold_record_id, booking_id, booking_status, customer_id, vehicle_type, 
                    payment_method, driver_ratings, booking_value, ride_distance, timestamp, 
                    month, day, hour, day_name, is_weekend, season, time_category,
                    has_driver_rating, has_customer_rating, revenue_per_km, distance_category, is_high_value
                ) VALUES (
                    :gid, :bid, 'Completed', :cid, :vt, :pm, :rate, :val, :dist, :ts,
                    :m, :d, :h, :dn, :iw, :s, :tc, 1, 0, :rpk, :dc, :ihv
                )
            """), {
                "gid": new_gid, "bid": bid, "cid": ride.customer_id, "vt": ride.vehicle_type.value,
                "pm": ride.payment_method.value, "rate": ride.driver_ratings, "val": calc_val,
                "dist": sim_dist, "ts": final_ts, "m": month, "d": dt_obj.day, "h": hour,
                "dn": day_name, "iw": is_weekend, "s": season, "tc": t_cat, "rpk": rev_per_km,
                "dc": dist_cat, "ihv": 1 if calc_val > avg_p else 0
            })
            conn.commit()
            return {"booking_id": bid, "details": {"price": calc_val, "distance": sim_dist}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ✅ UPDATE & DELETE (مشابه قبل)
@app.put("/rides/{booking_id}")
def update_status(booking_id: str, data: StatusUpdateSchema):
    with engine.connect() as conn:
        res = conn.execute(text("UPDATE gold.dataset SET booking_status = :s WHERE booking_id = :b"),
                           {"s": data.status.value, "b": booking_id.strip().replace('"', '')})
        conn.commit()
        if res.rowcount == 0: raise HTTPException(status_code=404)
    return {"message": "Updated"}


@app.delete("/rides/{booking_id}")
def delete_ride(booking_id: str):
    with engine.connect() as conn:
        res = conn.execute(text("DELETE FROM gold.dataset WHERE booking_id = :b"),
                           {"b": booking_id.strip().replace('"', '')})
        conn.commit()
        if res.rowcount == 0: raise HTTPException(status_code=404)
    return {"message": "Deleted"}