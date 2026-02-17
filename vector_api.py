from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import chromadb
from sqlalchemy import create_engine
from db_config import CONNECTION_STR
import pandas as pd

app = FastAPI(title="Uber Semantic Search API")
CHROMA_PATH = "./chroma_db"
COLLECTION_NAME = "cancellation_reasons"

chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = chroma_client.get_collection(name=COLLECTION_NAME)
pg_engine = create_engine(CONNECTION_STR)


class SearchQuery(BaseModel):
    query_text: str


@app.post("/search")
def semantic_search(search: SearchQuery):
    try:
        # جستجو در ChromaDB
        results = collection.query(
            query_texts=[search.query_text],
            n_results=5
        )

        if not results['ids'] or not results['ids'][0]:
            return {"message": "No similar records found.", "data": []}

        found_ids = results['ids'][0]

        # تبدیل به تاپل برای SQL
        ids_tuple = tuple(found_ids)

        if len(found_ids) == 1:
            sql_query = f"SELECT * FROM gold.dataset WHERE gold_record_id = {found_ids[0]}"
        else:
            sql_query = f"SELECT * FROM gold.dataset WHERE gold_record_id IN {ids_tuple}"

        df = pd.read_sql(sql_query, pg_engine)

        # مرتب‌سازی نتایج بر اساس ترتیبی که Chroma برگردانده (بهترین شباهت اول)
        # چون SQL ترتیب IN (...) را تضمین نمی‌کند
        df['gold_record_id'] = df['gold_record_id'].astype(str)  # همسان‌سازی نوع داده برای مرتب‌سازی
        df = df.set_index('gold_record_id')
        df = df.reindex(found_ids)
        df = df.reset_index()

        return df.fillna("").to_dict(orient="records")

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))