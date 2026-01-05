# extract_loba_async.py
# pip install openai pdfplumber pydantic pandas tqdm

import os, json, time, asyncio
from typing import Optional, List, Set

import pdfplumber
import pandas as pd
from pydantic import BaseModel, Field
from tqdm import tqdm
from openai import AsyncOpenAI

# =======================
# Constants / Globals
# =======================
PDF_PATH = os.environ.get("PDF_PATH", "data/loba-master.pdf")
MODEL = os.environ.get("MODEL", "gpt-5.2")

OUT_JSONL = os.environ.get("OUT_JSONL", "data/loba_rows.jsonl")
OUT_CSV = os.environ.get("OUT_CSV", "data/loba_rows.csv")

MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "6"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))
BACKOFF_BASE_SEC = float(os.environ.get("BACKOFF_BASE_SEC", "1.5"))

# Resume = skip pages already present in OUT_JSONL
RESUME = os.environ.get("RESUME", "1") == "1"

# Optional: process only a range like "0-299" (else all pages)
PAGE_RANGE = os.environ.get("PAGES", "").strip()  # e.g. "0-50" or ""

# =======================
# Schema
# =======================
class Row(BaseModel):
    cas_no: Optional[str] = None

    hsn_raw: Optional[str] = None          # e.g. "2915.2100" or "9802.0000"
    hsn_code: Optional[str] = None         # TB normalized: "29152100" (8 digits)

    name_raw: str
    name: str

    grade_percent: Optional[str] = None    # "98.0" or "20-30" (no %)
    grade_raw: Optional[str] = None        # e.g. "Extra Pure", "For Synthesis", "AR"
    grade_mapped: Optional[str] = None         # one of TB grade codes or null

    package_raw: str                       # exact package text, e.g. "1 Amp/1 Ltr", "25 gm", "3 Ampoules"
    size_value: float
    size_unit: str                         # TB normalized unit: gm/kg/ml/lt/mg/tbs/amp/pkt

    price: Optional[float] = None          # null if POR/POA/On Request

class Extract(BaseModel):
    rows: List[Row] = Field(default_factory=list)

SYSTEM = """
You extract structured rows from a chemical price list page.

Return JSON that matches the provided schema exactly.

GOAL
- Produce ONE row per (product × package/price line).

FIELD DEFINITIONS
- name_raw: the exact product HEADER line as it appears in the page (the title/bold line that starts a product block).
- name: chemical/product name only (remove purity/grade text).
- grade_percent: purity like "98%", "98.0%", "20-30%". Strip "%" and return only number/range (e.g. "98.0", "20-30"). Else null.
- grade_raw: any remaining grade descriptor from the header: "Extra Pure", "For Synthesis", "AR", "LR", "HPLC", etc. Else null.
- grade_mapped: map grade_raw to this allowed set if clearly applicable, else null:
  LR, AR, HPLC, SPEC, PR, GC, LCMS, DNA, MB, AAS, ICP, CVS, EL, GRAD, ACS

- cas_no: CAS number like 1234-56-7. If missing/NA, use "NA". Do NOT confuse HSN with CAS.
- hsn_raw: HSN/HS code as printed e.g. "2915.2100". If missing, null.
- hsn_code: normalize HSN to mapped format: 8 digits, no dots (e.g. "29152100"). If hsn_raw missing, null.

- package_raw: the exact package string on the price line (e.g. "25 gm", "1 Amp/1 Ltr", "3 Ampoules").
- size_unit: MUST be one of these mapped units (lowercase): gm, kg, ml, lt, mg, tbs, amp, pkt
- size_value: numeric for the chosen size_unit.

UNIT NORMALIZATION RULES
- Interpret "g/gram/grams" as gm.
- Interpret "l/L/ltr/litre/liters" as lt.
- Interpret "amp/ampoule/ampoules" as amp.
- Interpret "pkt/pack" as pkt.
- If package includes both count and volume (e.g. "1 Amp/1 Ltr"), choose the COUNT as primary:
  size_unit="amp", size_value=1, and keep package_raw exact.

HEADER CONTINUATION RULE
- If a header line ends with "For" and the next line is a short qualifier (e.g. "Microscopy"), append it to name_raw and treat as part of the header.

PRICE RULES
- price must be numeric if present.
- If price is POR/POA/On Request or missing, set price=null (never 0).

EXTRACTION RULES
- A product block starts at a HEADER; all following package/price lines belong to it until the next HEADER.
- Never use description/synonym lines as name_raw/name.
- Do not emit duplicates.

OUTPUT
- Return ONLY valid JSON. No prose, no markdown, no extra keys.
""".strip()


# =======================
# Helpers
# =======================
def parse_page_range(n_pages: int) -> List[int]:
    if not PAGE_RANGE:
        return list(range(n_pages))
    a, b = [int(x) for x in PAGE_RANGE.split("-")]
    a = max(0, a)
    b = min(n_pages - 1, b)
    return list(range(a, b + 1))

def extract_all_page_text(pdf_path: str) -> List[str]:
    # One pass read of the PDF text layer. (Avoid reopening PDF per task.)
    with pdfplumber.open(pdf_path) as pdf:
        return [(p.extract_text(layout=True) or "").strip() for p in pdf.pages]

def load_done_pages(jsonl_path: str) -> Set[int]:
    if not os.path.exists(jsonl_path):
        return set()
    if os.path.getsize(jsonl_path) == 0:
        return set()
    # pandas can read jsonl directly
    df = pd.read_json(jsonl_path, lines=True)
    return set(df["page_num"].astype(int).unique().tolist()) if "page_num" in df.columns else set()

def normalize_row_dict(d: dict) -> dict:
    # minimal normalization only (no CAS “fixing”)
    d["size_unit"] = str(d.get("size_unit", "")).strip().lower()
    return d

async def call_page_with_retry(
    client: AsyncOpenAI,
    sem: asyncio.Semaphore,
    page_num: int,
    txt: str,
) -> List[dict]:
    async with sem:
        last_err = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.responses.parse(
                    model=MODEL,
                    input=[
                        {"role": "system", "content": SYSTEM},
                        {"role": "user", "content": f"PAGE {page_num} TEXT:\n\n{txt}"},
                    ],
                    text_format=Extract,
                )
                rows = [normalize_row_dict(r.model_dump()) for r in resp.output_parsed.rows]
                # include page_num at row level for easy concatenation
                return [{"page_num": page_num, **r} for r in rows]
            except Exception as e:
                last_err = e
                sleep_s = min(30.0, BACKOFF_BASE_SEC * (2 ** attempt))
                print(f"[warn] page={page_num} attempt={attempt+1}/{MAX_RETRIES} error={type(e).__name__} sleeping={sleep_s:.1f}s")
                await asyncio.sleep(sleep_s)
        raise last_err

async def append_jsonl_locked(path: str, rows: List[dict], lock: asyncio.Lock) -> None:
    # Prevent interleaved writes from concurrent tasks.
    async with lock:
        with open(path, "a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv_from_jsonl(jsonl_path: str, csv_path: str) -> None:
    df = pd.read_json(jsonl_path, lines=True)
    if not df.empty:
        df = df.drop_duplicates()
    df.to_csv(csv_path, index=False)

# =======================
# Main
# =======================
async def main():
    if not os.path.exists(PDF_PATH):
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    print(f"[info] pdf={PDF_PATH}")
    print(f"[info] model={MODEL} max_conc={MAX_CONCURRENCY} retries={MAX_RETRIES} resume={RESUME}")
    print(f"[info] out_jsonl={OUT_JSONL} out_csv={OUT_CSV}")

    texts = extract_all_page_text(PDF_PATH)
    n_pages = len(texts)
    page_nums = parse_page_range(n_pages)

    done_pages = load_done_pages(OUT_JSONL) if RESUME else set()
    todo = [pn for pn in page_nums if pn not in done_pages]

    if not RESUME:
        # start fresh
        open(OUT_JSONL, "w", encoding="utf-8").close()
    else:
        # ensure file exists
        if not os.path.exists(OUT_JSONL):
            open(OUT_JSONL, "w", encoding="utf-8").close()

    print(f"[info] pages_total_in_pdf={n_pages} pages_selected={len(page_nums)} already_done={len(done_pages)} todo={len(todo)}")

    client = AsyncOpenAI()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    write_lock = asyncio.Lock()

    start = time.time()
    pbar = tqdm(total=len(todo), desc="Extracting", unit="page")

    async def run_one(pn: int):
        rows = await call_page_with_retry(client, sem, pn, texts[pn])
        await append_jsonl_locked(OUT_JSONL, rows, write_lock)
        return pn, len(rows)

    tasks = [asyncio.create_task(run_one(pn)) for pn in todo]

    completed = 0
    total_rows = 0
    for fut in asyncio.as_completed(tasks):
        pn, k = await fut
        completed += 1
        total_rows += k
        elapsed = time.time() - start
        ppm = (completed / elapsed) * 60 if elapsed > 0 else 0.0

        pbar.update(1)
        pbar.set_postfix({"last_page": pn, "rows": k, "ppm": f"{ppm:.1f}"})

        # lightweight logging
        if completed % 10 == 0 or completed == len(todo):
            print(f"[info] done={completed}/{len(todo)} total_rows={total_rows} ppm={ppm:.1f}")

    pbar.close()

    # Final CSV
    print("[info] writing CSV from JSONL ...")
    write_csv_from_jsonl(OUT_JSONL, OUT_CSV)
    print(f"[done] JSONL={OUT_JSONL} CSV={OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
