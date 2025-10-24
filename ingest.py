# ingest.py
import json
from pathlib import Path
from typing import List
import pypdf

DATA_DIR = Path("data")
OUT_FILE = Path("chunks.json")

def extract_text_from_pdf(path: Path) -> str:
    text = ""
    reader = pypdf.PdfReader(str(path))
    for p in reader.pages:
        page_text = p.extract_text()
        if page_text:
            text += page_text + "\n"
    return text

def extract_text_from_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def chunk_text(text: str, chunk_size_chars=2000, overlap_chars=200) -> List[str]:
    chunks = []
    i = 0
    L = len(text)
    while i < L:
        chunk = text[i:i+chunk_size_chars]
        chunks.append(chunk.strip())
        i += chunk_size_chars - overlap_chars
    return chunks

def main():
    DATA_DIR.mkdir(exist_ok=True)
    all_chunks = []
    id_counter = 0
    for p in sorted(DATA_DIR.iterdir()):
        if p.suffix.lower() == ".pdf":
            txt = extract_text_from_pdf(p)
        elif p.suffix.lower() in [".txt", ".md"]:
            txt = extract_text_from_txt(p)
        else:
            continue
        if not txt.strip():
            continue
        chunks = chunk_text(txt)
        for i, ch in enumerate(chunks):
            all_chunks.append({
                "id": f"{p.name}__chunk_{i}",
                "text": ch,
                "meta": {"source": p.name, "chunk_index": i}
            })
            id_counter += 1

    if not all_chunks:
        print("No documents found in data/ or content was empty.")
        return

    OUT_FILE.write_text(json.dumps(all_chunks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(all_chunks)} chunks to {OUT_FILE}")

if __name__ == "__main__":
    main()
