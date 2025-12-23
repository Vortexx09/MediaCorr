from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.ingestor import ingest_source
from app.sources import RSS_SOURCES

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message":"Backend FastAPI funcionando"}

@app.post("/ingest")
def ingest_news(sources: list[str]):
    all_articles = []

    for source in sources:
        rss_url = RSS_SOURCES.get(source)
        if rss_url:
            articles = ingest_source(source, rss_url)
            all_articles.extend(articles)

    return {
        "count": len(all_articles),
        "data" : all_articles,
    }
