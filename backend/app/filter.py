from app.config.keywords import KEYWORDS

def filter_news(parsed_records, keywords=KEYWORDS):
    filtered = []
    for record in parsed_records:
        text = (record.get("title") or "") + " " + (record.get("body") or "")
        if any(kw.lower() in text.lower() for kw in keywords):
            filtered.append(record)
    print(f"[INFO] Filtradas {len(filtered)} noticias de {len(parsed_records)}")
    return filtered
