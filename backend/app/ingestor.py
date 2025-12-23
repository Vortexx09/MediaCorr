import feedparser
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

def fetch_article(entry, source_name):
    try:
        response = requests.get(entry.link, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        paragraphs = soup.find_all("p")
        content = " ".join(p.get_text() for p in paragraphs)

        return {
            "source": source_name,
            "url": entry.link,
            "title": entry.title,
            "date": entry.get("published", ""),
            "content": content
        }
    except Exception as e:
        return None
    
def ingest_source(source_name, rss_url):
    feed = feedparser.parse(rss_url)
    articles = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(fetch_article, entry, source_name)
            for entry in feed.entries
        ]

        for future in futures:
            result = future.result()
            if result:
                articles.append(result)

    return articles