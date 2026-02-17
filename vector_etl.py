import pandas as pd
import chromadb
from sqlalchemy import create_engine
from db_config import CONNECTION_STR
import os

# تنظیمات
CHROMA_PATH = "./chroma_db"
COLLECTION_NAME = "cancellation_reasons"


def load_data_to_chroma():
    print("⏳ Connecting to PostgreSQL...")
    engine = create_engine(CONNECTION_STR)

    query = """
    SELECT gold_record_id, booking_id, unified_cancellation_reason 
    FROM gold.dataset 
    WHERE unified_cancellation_reason IS NOT NULL 
      AND unified_cancellation_reason != ''
    """
    df = pd.read_sql(query, engine)
    print(f"✅ Fetched {len(df)} records with cancellation reasons.")

    # حذف دیتابیس قبلی اگر خراب شده است
    if os.path.exists(CHROMA_PATH):
        pass

    print("⏳ Initializing ChromaDB...")
    client = chromadb.PersistentClient(path=CHROMA_PATH)

    try:
        client.delete_collection(name=COLLECTION_NAME)
    except:
        pass

    collection = client.create_collection(name=COLLECTION_NAME)

    batch_size = 5000
    total_records = len(df)

    print("🚀 Starting Vectorization and Insertion...")

    for i in range(0, total_records, batch_size):
        batch = df.iloc[i: i + batch_size]

        documents = batch['unified_cancellation_reason'].tolist()

        # ChromaDB نیاز دارد ID حتما رشته (String) باشد
        ids = [str(x) for x in batch['gold_record_id'].tolist()]

        # بوکینگ آی‌دی را در متادیتا نگه می‌داریم تا گم نشود
        metadatas = [{"booking_id": bid} for bid in batch['booking_id'].tolist()]

        collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
        print(f"   Processed {min(i + batch_size, total_records)} / {total_records}")

    print("🎉 ETL Completed! Data is now indexed in ChromaDB.")


if __name__ == "__main__":
    load_data_to_chroma()