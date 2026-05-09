from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from datetime import datetime
import tempfile
import re
import shutil

from PIL import Image
import pytesseract
from rapidfuzz import fuzz
from dataclasses import dataclass
from typing import List, Dict

from write_to_postgres import write_to_postgres


PREFIX = "data/"

INVOICE_SCHEMA = {
    "invoice_number": [
        "invoice no",
        "invoice number",
        "Invoice no:"
    ],
    "invoice_date": [
        "date",
        "invoice date"
    ],
    "total_amount": [
        "total",
        "amount due",
        "balance due",
        "grand total",
        "Total"
    ],
}

@dataclass
class Token:
    text: str
    x: int
    y: int
    w: int
    h: int
    conf: float
    line_id: int


def normalize(s: str) -> str:
    return "".join(
        ch.lower()
        for ch in s
        if ch.isalnum() or ch.isspace()
    )


def normalize_money(value: str) -> str:
    return value.replace(",", ".")


def extract_total_amount(text: str) -> str:

    lines = text.split("\n")
    candidates = []

    for line in lines:
        if "total" in line.lower():
            nums = re.findall(r"\d+[.,]\d{2}", line)
            candidates.extend(nums)

    if not candidates:
        all_nums = re.findall(r"\d+[.,]\d{2}", text)

        if all_nums:
            candidates = all_nums

    if not candidates:
        return ""

    return normalize_money(candidates[-1])


def best_keyword_match(
    text: str,
    candidates: List[str],
    cutoff=75
):
    best_score = 0
    best_kw = ""

    norm_text = normalize(text)

    for kw in candidates:
        score = fuzz.partial_ratio(
            norm_text,
            normalize(kw)
        )

        if score > best_score:
            best_score = score
            best_kw = kw

    return best_kw, best_score if best_score >= cutoff else 0


def ocr_tokens(image: Image.Image) -> List[Token]:

    data = pytesseract.image_to_data(
        image,
        output_type=pytesseract.Output.DICT
    )

    tokens = []

    for i in range(len(data["text"])):

        text = data["text"][i].strip()

        if not text:
            continue

        conf = (
            float(data["conf"][i])
            if data["conf"][i] != "-1"
            else 0.0
        )

        line_id = hash((
            data["block_num"][i],
            data["par_num"][i],
            data["line_num"][i]
        ))

        tokens.append(
            Token(
                text=text,
                x=data["left"][i],
                y=data["top"][i],
                w=data["width"][i],
                h=data["height"][i],
                conf=conf,
                line_id=line_id
            )
        )

    return tokens


def join_lines(tokens: List[Token]):

    lines = {}

    for t in tokens:
        lines.setdefault(t.line_id, []).append(t)

    for lid in lines:
        lines[lid] = sorted(
            lines[lid],
            key=lambda t: t.x
        )

    return lines


def extract_near_keyword(lines, keywords):

    sorted_lines = sorted(
        lines.items(),
        key=lambda x: min(t.y for t in x[1])
    )

    for _, toks in sorted_lines:

        full_line = " ".join(
            t.text for t in toks
        )

        kw, score = best_keyword_match(
            full_line,
            keywords
        )

        if score > 0 and kw.lower() in full_line.lower():

            value = (
                full_line
                .split(kw, 1)[-1]
                .strip(": ")
                .strip()
            )

            if value:
                return value

    return ""


def extract_receipt_fields(text: str) -> Dict[str, str]:

    patterns = {
        "total_amount": r"TOTAL\s*\$?\s*(\d+[.,]\d{2})",
        "invoice_date": r"(\d{1,2}/\d{1,2}/\d{4})",
        "invoice_number": r"Invoice no:\s*(\d+)",
    }

    results = {}

    for k, p in patterns.items():

        match = re.search(
            p,
            text,
            re.IGNORECASE
        )

        if match:
            results[k] = match.group(
                1 if match.lastindex else 0
            )

    return results


@task
def list_invoice_files() -> List[str]:

    hook = S3Hook(
        aws_conn_id='minio_default'
    )

    keys = hook.list_keys(
        bucket_name='warehouse',
        prefix=PREFIX
    )

    image_files = [
        k for k in keys
        if k.lower().endswith(
            (
                '.png',
                '.jpg',
                '.jpeg',
                '.tiff',
                '.bmp'
            )
        )
    ]

    print(f"Found {len(image_files)} image files")

    return image_files


@task
def process_image(s3_key: str) -> Dict:

    bucket = "warehouse"

    print(f"Processing: {s3_key}")

    hook = S3Hook(
        aws_conn_id='minio_default'
    )

    tmp_dir = tempfile.mkdtemp()

    try:

        local_file = hook.download_file(
            key=s3_key,
            bucket_name=bucket,
            local_path=tmp_dir
        )

        image = Image.open(
            local_file
        ).convert("L")

        full_text = pytesseract.image_to_string(
            image
        )

        tokens = ocr_tokens(image)

        lines = join_lines(tokens)

        invoice_data = {
            field: extract_near_keyword(
                lines,
                cues
            )
            for field, cues in INVOICE_SCHEMA.items()
        }

        receipt_data = extract_receipt_fields(
            full_text
        )

        # Better total extraction
        receipt_data["total_amount"] = (
            extract_total_amount(full_text)
        )

        result = {
            "s3_key": s3_key,
            "extracted": {
                **invoice_data,
                **receipt_data
            }
        }

        print(f"Extracted: {s3_key}")

        return result

    finally:
        shutil.rmtree(
            tmp_dir,
            ignore_errors=True
        )


with DAG(
    dag_id='ocr_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 2,
        'owner': 'airflow'
    },
) as dag:

    files = list_invoice_files()

    processed = process_image.expand(
        s3_key=files
    )

    written = write_to_postgres.expand(
        result=processed
    )