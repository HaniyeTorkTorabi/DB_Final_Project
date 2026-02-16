import streamlit as st
from chat_logic import get_sql_from_ai

# تنظیمات صفحه
st.set_page_config(page_title="Uber AI Chat", page_icon="🤖")

st.title("🤖 Uber Intelligent SQL Assistant")
st.markdown("---")
st.info("Please ask your questions in English only.")

# مدیریت حافظه چت
if "messages" not in st.session_state:
    st.session_state.messages = []

# نمایش پیام‌های قبلی
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            # نمایش کد SQL با فرمت مناسب
            st.code(message["content"], language="sql")
        else:
            st.markdown(message["content"])

# دریافت سوال از کاربر
if prompt := st.chat_input("Ask about the data (e.g., 'Total value for Uber XL in Winter')"):
    # ۱. نمایش پیام کاربر
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # ۲. دریافت و نمایش پاسخ هوش مصنوعی
    with st.chat_message("assistant"):
        with st.spinner("Generating SQL Query..."):
            ai_sql = get_sql_from_ai(prompt)
            st.code(ai_sql, language="sql")

            # ذخیره در تاریخچه
            st.session_state.messages.append({"role": "assistant", "content": ai_sql})

# نمایش یک تذکر کوچک در پایین صفحه
st.sidebar.caption("Powered by OpenRouter & Gemini 2.0")
st.sidebar.write("Step 7: Natural Language to SQL")