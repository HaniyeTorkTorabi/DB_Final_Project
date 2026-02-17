import streamlit as st
import requests
import pandas as pd
import os

os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# --- تنظیمات برنامه ---
API_URL = "http://localhost:8002/search"

st.set_page_config(page_title="Semantic Search", page_icon="🔍", layout="wide")

# هدر و توضیحات
st.title("🔍 Uber Semantic Search Engine")
st.markdown("Find cancelled rides by **meaning**, not just keywords.")
st.markdown("---")

# ورودی کاربر
col1, col2 = st.columns([3, 1])
with col1:
    query = st.text_input("Describe the cancellation reason:", placeholder="e.g., 'Driver was rude' or 'Car broken'")
with col2:
    st.write("")  # فاصله خالی برای تراز شدن دکمه
    st.write("")
    search_btn = st.button("Search Meaning", type="primary", use_container_width=True)

if search_btn:
    if query:
        with st.spinner("Searching in Vector Database & Fetching from PostgreSQL..."):
            try:
                # ارسال درخواست به API
                # چون no_proxy را ست کردیم، این درخواست مستقیم به پورت 8002 می‌رود
                response = requests.post(API_URL, json={"query_text": query})

                if response.status_code == 200:
                    data = response.json()

                    if not data or (isinstance(data, dict) and "message" in data):
                        st.warning("No records found similar to your description.")
                    else:
                        st.success(f"Found {len(data)} similar rides!")

                        # نمایش نتایج
                        for ride in data:
                            with st.container():
                                st.markdown("### 🚗 Ride Details")
                                c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
                                c1.metric("Vehicle", ride.get('vehicle_type', 'N/A'))
                                c2.metric("Price", f"${ride.get('booking_value', 0)}")
                                c3.metric("Distance", f"{ride.get('ride_distance', 0)} km")
                                c4.metric("Status", ride.get('booking_status', 'N/A'))

                                st.info(f"**Cancellation Reason:** {ride.get('unified_cancellation_reason')}")
                                st.markdown(
                                    f"<small>ID: {ride.get('booking_id')} | Date: {ride.get('timestamp')}</small>",
                                    unsafe_allow_html=True)
                                st.divider()
                else:
                    st.error(f"Server Error: {response.text}")
            except requests.exceptions.ConnectionError:
                st.error("❌ Could not connect to the Backend API. Is 'vector_api.py' running?")
            except Exception as e:
                st.error(f"An error occurred: {e}")
    else:
        st.warning("⚠️ Please enter a search query first.")
