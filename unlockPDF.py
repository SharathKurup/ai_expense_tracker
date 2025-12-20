import pikepdf
from pathlib import Path
import re
import os
from dotenv import load_dotenv

load_dotenv()

INPUT_DIR = Path(os.getenv("INPUT_DIR", "attachments/locked"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "attachments/unlocked"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PASSWORDS = os.getenv("PDF_PASSWORDS", "").split(",")

def normalize_filename(pdf_path: Path) -> str:
    name = pdf_path.stem.lower()

    month_year_match = re.search(
        r"(january|february|march|april|may|june|"
        r"july|august|september|october|november|december)"
        r"\s+(\d{4})",
        name
    )

    if month_year_match:
        month, year = month_year_match.groups()
        return f"axisbank_statement_{month}_{year}.pdf"

    date_range_match = re.search(
        r"(\d{2})-(\d{2})-(\d{4})to(\d{2})-(\d{2})-(\d{4})",
        name
    )

    if date_range_match:
        # start date defines the month
        _, month, year, *_ = date_range_match.groups()
        month_map = {
            "01": "january", "02": "february", "03": "march",
            "04": "april", "05": "may", "06": "june",
            "07": "july", "08": "august", "09": "september",
            "10": "october", "11": "november", "12": "december"
        }
        return f"axisbank_statement_{month_map[month]}_{year}.pdf"

    raise ValueError(f"Cannot normalize filename: {pdf_path.name}")


for pdf_file in INPUT_DIR.glob("*.pdf"):
    print(f"Processing file: {pdf_file.name}")
    pdf = None

    for pwd in PASSWORDS:
        try:
            pdf = pikepdf.open(pdf_file, password=pwd)
            print("  ✔ Password matched")
            break
        except pikepdf.PasswordError:
            continue

    if pdf is None:
        print("  ✖ Failed to unlock PDF with provided passwords")
        continue

    output_name = normalize_filename(pdf_file)
    output_path = OUTPUT_DIR / output_name

    pdf.save(output_path)
    pdf.close()

    print(f"  ✔ Saved to: {output_path}")

print("Processing completed.")
