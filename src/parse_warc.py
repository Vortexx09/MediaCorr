import os
import gzip
import json
from datetime import datetime, timezone
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import langid
from tqdm import tqdm

from utils import ensure_dir

def clean_text(html_bytes: bytes) -> tuple[str, str]:
    try:
        html = html_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = html_bytes.decode("latin-1", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    # Remove scripts/styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # Join visible text
    text = " ".join(soup.get_text(separator=" ").split())
    return title, text

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def parse_warc_file(path: str, out_jsonl: str):
    count = 0
    with gzip.open(path, "rb") as stream, open(out_jsonl, "w", encoding="utf-8") as out:
        for record in tqdm(ArchiveIterator(stream), desc=os.path.basename(path)):
            if record.rec_type != "response":
                continue
            url = record.rec_headers.get_header("WARC-Target-URI")
            date = record.rec_headers.get_header("WARC-Date")
            payload = record.content_stream().read()
            title, text = clean_text(payload)
            if not text or len(text) < 300:
                # Skip very small pages (likely non-articles or listing pages)
                continue
            lang, _ = langid.classify(text)
            doc = {
                "id": f"{os.path.basename(path)}::{count}",
                "source": "ccnews",
                "url": url,
                "title": title,
                "published_at": date or iso_now(),
                "language": lang,
                "raw_html_bytes": len(payload),  # store length for debugging
                "text": text,
                "warc_file": os.path.basename(path)
            }
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
    return count

def main(input_dir="data/raw", output_dir="data/parsed"):
    ensure_dir(output_dir)
    files = [f for f in os.listdir(input_dir) if f.endswith(".warc.gz")]
    if not files:
        print(f"No WARC files in {input_dir}. Download first.")
        return
    total_docs = 0
    for fname in files:
        in_path = os.path.join(input_dir, fname)
        out_path = os.path.join(output_dir, fname.replace(".warc.gz", ".jsonl"))
        print(f"[parse] {in_path} -> {out_path}")
        docs = parse_warc_file(in_path, out_path)
        print(f"  Extracted {docs} articles.")
        total_docs += docs
    print(f"Done. Total extracted: {total_docs}")

if __name__ == "__main__":
    main()
