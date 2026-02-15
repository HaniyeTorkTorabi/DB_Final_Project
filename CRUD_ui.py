import streamlit as st
import requests
import pandas as pd
import datetime
import os
import re

# رفع مشکل VPN
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

API_URL = "http://127.0.0.1:8000"

st.title("🚖 پنل مدیریت و غنی‌سازی داده‌های Uber")

st.set_page_config(
    page_title="Uber Gold Panel",
    page_icon="🚖",           # 👈 این همان ماشینی است که غیب شده بود!
    layout="wide"
)

# تب‌ها با نام‌های دوزبانه
tabs = st.tabs([
    "📋 مشاهده و فیلتر (Read)",
    "➕ ثبت سفر هوشمند (Create)",
    "✏️ ویرایش وضعیت (Update)",
    "❌ حذف سفر (Delete)"
])

# --- TAB 1: READ ---
with tabs[0]:
    # قرار دادن باکس‌ها و دکمه در یک ردیف
    c1, c2, c3, c4 = st.columns([2, 0.6, 0.8, 0.7], gap="small")

    with c1:
        cid_search = st.text_input("🔍 جستجو Customer ID:", placeholder="CID1234567")
    with c2:
        lim = st.number_input("تعداد نمایش:", 1, 1000, 50)
    with c3:
        st.write("")  # ایجاد فاصله برای تراز شدن با باکس‌ها
        st.write("")
        all_cols = st.checkbox("نمایش تمامی ستون‌ها")
    with c4:
        st.write("")  # ایجاد فاصله برای تراز شدن
        st.write("")
        load_btn = st.button("🔄 دریافت داده‌ها")

    if load_btn or cid_search:
        res = requests.get(f"{API_URL}/rides/", params={"customer_id": cid_search, "limit": lim})
        if res.status_code == 200:
            df = pd.DataFrame(res.json())
            if not df.empty:
                st.success(f"✅ {len(df)} رکورد یافت شد.")
                # نمایش ستون‌های منتخب یا کل ستون‌ها
                display_df = df if all_cols else df[
                    ["booking_id", "booking_status", "customer_id", "vehicle_type", "booking_value", "ride_distance",
                     "timestamp"]]
                st.dataframe(display_df, use_container_width=True)
            else:
                st.warning("⚠️ داده‌ای با این مشخصات یافت نشد.")

# --- CREATE ---
with tabs[1]:
    st.info("فصل، بازه زمانی، نام روز، مسافت و قیمت به صورت خودکار محاسبه و در دیتابیس ذخیره می‌شوند.")
    with st.form("create_form"):
        col1, col2 = st.columns(2)
        d = col1.date_input("تاریخ")
        t = col1.time_input("زمان")
        cid = col2.text_input("Customer ID (CID + 7 رقم)", "CID1000000")
        vt = col2.selectbox("نوع خودرو", ["Auto", "Premier Sedan", "Go Sedan", "eBike", "Bike", "Go Mini"])
        pm = col1.selectbox("روش پرداخت", ["Cash", "UPI", "Card", "Wallet"])
        rate = col2.slider("امتیاز به راننده", 0.0, 5.0, 5.0)

        if st.form_submit_button("✅ محاسبه و ثبت نهایی"):
            if not re.match(r"^CID\d{7}$", cid):
                st.error("فرمت CID اشتباه است (باید CID و ۷ رقم باشد)")
            else:
                payload = {"date": d.strftime("%m/%d/%Y"), "time": t.strftime("%H:%M:%S"), "customer_id": cid,
                           "vehicle_type": vt, "payment_method": pm, "driver_ratings": rate}
                res = requests.post(f"{API_URL}/rides/", json=payload)
                if res.status_code == 201:
                    r = res.json()
                    st.success(f"ثبت شد! ID: {r['booking_id']}")
                    st.metric("قیمت محاسبه شده", f"{r['details']['price']:,} واحد")
                    st.metric("مسافت تخمینی", f"{r['details']['distance']} km")

# --- UPDATE & DELETE ---
with tabs[2]:
    ubid = st.text_input("Booking ID برای ویرایش:")
    stat = st.selectbox("وضعیت جدید", ["Completed", "Cancelled by Customer", "Cancelled by Driver", "Incomplete"])
    if st.button("بروزرسانی"):
        requests.put(f"{API_URL}/rides/{ubid}", json={"status": stat})
        st.success("انجام شد.")

# --- DELETE (حذف) ---
with tabs[3]:
    # 🔴 این همان هشداری است که درباره‌اش صحبت کردیم:
    st.warning("⚠️ **هشدار امنیتی:** عملیات حذف به هیچ عنوان قابل بازگشت نیست. لطفاً در وارد کردن Booking ID دقت کنید.")

    dbid = st.text_input("Booking ID برای حذف:", placeholder="مثلاً CNR1234567")

    if st.button("🗑️ حذف دائمی رکورد"):
        if dbid:
            res = requests.delete(f"{API_URL}/rides/{dbid}")
            if res.status_code == 200:
                st.success(f"✅ رکورد با شناسه {dbid} با موفقیت از دیتابیس حذف شد.")
            elif res.status_code == 404:
                st.error("❌ خطای ۴۰۴: این شناسه در دیتابیس یافت نشد.")
            else:
                st.error(f"خطا در حذف: {res.text}")
        else:
            st.info("لطفاً ابتدا یک Booking ID معتبر وارد کنید.")