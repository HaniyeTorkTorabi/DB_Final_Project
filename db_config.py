import urllib.parse

# --- اطلاعات اتصال ---
DB_USER = "postgres"
DB_PASS = "@3084314"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "uber_project"

# رمزگذاری کاراکترهای خاص در پسورد (مثل @) برای جلوگیری از خطا
encoded_pass = urllib.parse.quote_plus(DB_PASS)

# رشته اتصال نهایی
CONNECTION_STR = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
