import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
import datetime

# تنظیمات صفحه
st.set_page_config(page_title="Uber Analytics", page_icon="📈", layout="wide")

# تنظیم پروکسی
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

API_URL = "http://127.0.0.1:8001"

# ==========================================
# 🛠️ سایدبار فیلترها
# ==========================================
st.sidebar.header("🔍 فیلترهای داشبورد")

# ✅ دکمه بروزرسانی دستی (بخش جدید)
if st.sidebar.button("🔄 بروزرسانی داده‌ها", type="primary"):
    st.cache_data.clear()  # پاک کردن حافظه موقت برای دیدن تغییرات جدید
    st.rerun()  # اجرای مجدد برنامه

st.sidebar.markdown("---")

# 1. فیلتر بازه زمانی
# پیش‌فرض روی گذشته تا آینده (برای پوشش دیتای 2026)
# اگر می‌خواهید هوشمند باشد، کد fetch_metadata قبلی را جایگزین کنید
# اما اینجا طبق کد دستی شما پیش رفتیم:
last_month = datetime.date(2024, 1, 1)
start_date = st.sidebar.date_input("📅 از تاریخ:", value=last_month)
end_date = st.sidebar.date_input("📅 تا تاریخ:", value=datetime.date(2026, 12, 30))

# 2. فیلتر نوع خودرو (شامل Uber XL)
vehicle_options = ["Auto", "Premier Sedan", "Go Sedan", "eBike", "Bike", "Go Mini", "Uber XL"]
selected_vehicles = st.sidebar.multiselect(
    "🚖 نوع خودرو:",
    options=vehicle_options,
    default=vehicle_options  # پیش‌فرض همه انتخاب شده‌اند
)


# آماده‌سازی پارامترها برای ارسال به API
params = {
    "start_date": str(start_date),
    "end_date": str(end_date),
    "vehicles": selected_vehicles
}


# ==========================================
# توابع دریافت داده
# ==========================================
def get_data(endpoint):
    try:
        # ارسال پارامترها به API
        res = requests.get(f"{API_URL}{endpoint}", params=params)
        if res.status_code == 200:
            return res.json()
        return None
    except:
        return None


# ==========================================
# بدنه اصلی داشبورد
# ==========================================
st.title("📊 داشبورد تحلیل داده‌های Uber")
st.markdown(f"نمایش آمار برای بازه: **{start_date}** تا **{end_date}**")
st.markdown("---")

# 1. KPI Section
kpi = get_data("/analytics/kpi")
if kpi:
    st.subheader("۱. شاخص‌های کلیدی عملکرد (KPIs)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("کل سفرها", f"{kpi['total_bookings']:,}")
    c2.metric("سفرهای موفق", f"{kpi['successful_bookings']:,}")
    c3.metric("درآمد کل", f"{kpi['total_revenue']:,} تومان")
    c4.metric("نرخ موفقیت", f"{kpi['success_rate']}%")
    st.markdown("---")

# 2. Pie Charts (با قابلیت بزرگنمایی واکنشی)
pie = get_data("/analytics/pie-data")
if pie:
    st.subheader("۲. تحلیل توزیع داده‌ها")

    # دو ستون برای نمایش کنار هم
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### 🔸 دلایل دقیق لغو سفر")
        df_c = pd.DataFrame(pie['cancellations'])
        if not df_c.empty:
            # ✅ اینجا تغییر کرد: unified_cancellation_reason
            fig1 = px.pie(df_c, values='count', names='unified_cancellation_reason', hole=0.4,
                          color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("داده‌ای برای لغو سفر در این بازه نیست.")

    with col2:
        st.markdown("##### 💳 روش‌های پرداخت")
        df_p = pd.DataFrame(pie['payments'])
        if not df_p.empty:
            fig2 = px.pie(df_p, values='count', names='payment_method', hole=0.4,
                          color_discrete_sequence=px.colors.sequential.Teal,
                          height=500)
            fig2.update_traces(textposition='inside', textinfo='percent+label')
            fig2.update_layout(legend=dict(orientation="h", y=-0.1))  # لجند پایین
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("داده‌ای برای روش پرداخت موجود نیست.")

# 3. Bar Charts
bar = get_data("/analytics/bar-data")
if bar:
    st.subheader("۳. عملکرد ناوگان")
    df_b = pd.DataFrame(bar)
    if not df_b.empty:
        df_b['avg_driver'] = df_b['avg_driver'].fillna(0).round(2)
        df_b['avg_customer'] = df_b['avg_customer'].fillna(0).round(2)

        tab1, tab2 = st.tabs(["تعداد سفرها", "مقایسه امتیازات"])

        with tab1:
            fig3 = px.bar(df_b, x='vehicle_type', y='trip_count', color='vehicle_type', text='trip_count',
                          title="تعداد سفرها")
            st.plotly_chart(fig3, use_container_width=True)

        with tab2:
            df_melted = df_b.melt(id_vars=['vehicle_type'], value_vars=['avg_driver', 'avg_customer'], var_name='Type',
                                  value_name='Score')
            fig4 = px.bar(df_melted, x='vehicle_type', y='Score', color='Type', barmode='group', text='Score',
                          range_y=[0, 5.5],
                          color_discrete_map={'avg_driver': '#1f77b4', 'avg_customer': '#ff7f0e'})
            st.plotly_chart(fig4, use_container_width=True)
    else:
        st.warning("هیچ سفری با فیلترهای انتخاب شده یافت نشد.")

# 4. Line Charts
line = get_data("/analytics/line-data")
if line:
    st.subheader("۴. تحلیل زمانی تردد")
    c_a, c_b = st.columns(2)

    with c_a:
        df_h = pd.DataFrame(line['hourly'])
        if not df_h.empty:
            fig5 = px.line(df_h, x='hour', y='count', markers=True, title="ساعات شلوغی")
            fig5.update_traces(line_color='#FF4B4B')
            st.plotly_chart(fig5, use_container_width=True)

    with c_b:
        df_d = pd.DataFrame(line['daily'])
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        df_d['day_name'] = pd.Categorical(df_d['day_name'], categories=days_order, ordered=True)
        df_d = df_d.sort_values('day_name')
        if not df_d.empty:
            fig6 = px.line(df_d, x='day_name', y='count', markers=True, title="روزهای هفته")
            st.plotly_chart(fig6, use_container_width=True)